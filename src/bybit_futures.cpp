/**
Bybit Futures Zorro Plugin

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stdafx.h"
#include "stonky/bybit/bybit_rest_client.h"
#include "stonky/bybit/bybit_ws_stream_manager.h"
/// NOTE: every Boost header must be included before bybit_futures.h - Zorro's trading.h defines the alternative
/// operator tokens (and, or, not) as macros, which breaks Boost headers included after it.
#include "stonky/bybit/bybit_http_session.h"
#include "stonky/utils/utils.h"
#include "stonky/utils/registry.h"
#include "bybit_futures.h"
#include <wtypes.h>
#include <string>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <optional>

#include "stonky/bybit/bybit.h"

#define PLUGIN_VERSION    2
#define PLUGIN_VERSION_STR "2.0.0"
#define PLUGIN_VERSION_RELEASE_DATE "29-august-2026"
#undef min

#define ZORRO_REG_KEY "SOFTWARE\\Zorro"
#define LAST_ORDER_ID_KEY "BybitLastOrderId"
#define OPEN_TRADES_FILE R"(./Data/bybit_open_trades.json)"

using namespace std::chrono_literals;
using namespace stonky::bybit;

/// Maximum time spent waiting for a price of an asset before the REST snapshot fallback kicks in
static constexpr int STREAM_READ_TIMEOUT_S = 5;

/// A cached quote older than this is considered unusable and is refreshed over REST. Must be generous enough for
/// illiquid symbols whose top of book legitimately does not move for a long time.
static constexpr int MAX_TICK_AGE_S = 60;

/// Period of the background refresh of the instrument info (lot sizes, tick sizes, ...)
static constexpr int INSTRUMENTS_UPDATE_PERIOD_S = 900;

/// Bybit's order create response carries no status, it has to be queried. A resting or terminal state is normally
/// known on the first poll.
static constexpr int MAX_ORDER_POLL_ATTEMPTS = 10;
static constexpr auto ORDER_POLL_INTERVAL = 250ms;

/// Successful traffic newer than this counts as proof that the connection is alive, so BrokerTime does not have to
/// probe the exchange on every call
static constexpr std::int64_t CONNECTION_FRESH_MS = 30000;

/// Minimum spacing between connection probes once the traffic went quiet
static constexpr std::int64_t CONNECTION_PROBE_INTERVAL_MS = 10000;

static std::string currentSymbol;
static std::string accountCurrency;
static int lastOrderId = 0;
static int orderType = 0;
/// Whether the account keeps long and short legs separately. Bybit has no account wide query for it, so it is
/// learned from the position records when orders are placed; One-way, the venue default, is the safe assumption.
static bool hedge = false;
static double lotAmount = 1.0;
static int loopMs = 50; // Zorro loop time, kept for GET_DELAY only
static int waitMs = STREAM_READ_TIMEOUT_S * 1000; // Maximum broker response time, drives the stream read timeout

std::atomic symbolsUpdaterRunning = false;
std::thread symbolsUpdater;

std::shared_ptr<RESTClient> restClient;
std::unique_ptr<WSStreamManager> streamManager;

#ifdef _WIN64
std::map<std::string, double> lotAmounts;
#endif

enum ExchangeStatus {
	Unavailable = 0,
	Closed,
	Open
};

__int64 convertTime(const DATE Date) {
	return static_cast<__int64>((Date - 25569.) * 24. * 60. * 60.) * 1000;
}

DATE convertTime(const __int64 t64) {
	if (t64 == 0) return 0.;
	return 25569. + static_cast<double>(t64 / 1000) / (24. * 60. * 60.);
}

DLLFUNC_C int BrokerOpen(char *Name, FARPROC fpError, FARPROC fpProgress) {
	strcpy_s(Name, 32, "BybitFutures");
	(FARPROC &) BrokerError = fpError;
	(FARPROC &) BrokerProgress = fpProgress;

	return PLUGIN_VERSION;
}

void logFunction(const stonky::LogSeverity severity, const std::string &errmsg) {
	switch (severity) {
		case stonky::LogSeverity::Info:
			spdlog::info(errmsg);
			break;
		case stonky::LogSeverity::Warning:
			spdlog::warn(errmsg);
			break;
		case stonky::LogSeverity::Critical:
			spdlog::critical(errmsg);
			break;
		case stonky::LogSeverity::Error:
			spdlog::error(errmsg);
			break;
		case stonky::LogSeverity::Debug:
			spdlog::debug(errmsg);
			break;
		case stonky::LogSeverity::Trace:
			spdlog::trace(errmsg);
			break;
	}
}

void symbolsUpdaterFunc() {
	symbolsUpdaterRunning = true;

	/// Refresh right away, then periodically. The instrument list covers the whole linear universe, downloading it
	/// more often than the filters can realistically change is a waste of bandwidth and rate limit budget.
	int numPass = INSTRUMENTS_UPDATE_PERIOD_S;

	while (symbolsUpdaterRunning) {
		if (numPass >= INSTRUMENTS_UPDATE_PERIOD_S) {
			numPass = 0;

			try {
				if (restClient) {
					static_cast<void>(restClient->getInstrumentsInfo(Category::linear, "", true));
				}
			} catch (std::exception &e) {
				spdlog::error("{}: {}", MAKE_FILELINE, e.what());
			}
		}
		std::this_thread::sleep_for(1s);
		numPass++;
	}
}

void startSymbolsUpdater() {
	symbolsUpdater = std::thread(&symbolsUpdaterFunc);
}

void stopSymbolsUpdater() {
	symbolsUpdaterRunning = false;

	if (symbolsUpdater.joinable()) {
		symbolsUpdater.join();
	}
}

void writeBybitLastOrderId() {
	if (const bool success = stonky::writeInRegistry(HKEY_CURRENT_USER, ZORRO_REG_KEY, LAST_ORDER_ID_KEY, lastOrderId); !
		success) {
		spdlog::error("Cannot store BybitLastOrderId");
	}
}

void readBybitLastOrderId() {
	DWORD bybitLastOrderId;
	const bool success = stonky::readDwordValueRegistry(HKEY_CURRENT_USER, ZORRO_REG_KEY, LAST_ORDER_ID_KEY,
	                                           &bybitLastOrderId);
	if (success && bybitLastOrderId != 0) {
		lastOrderId = static_cast<int>(bybitLastOrderId);
	} else {
		time_t Time;
		time(&Time);
		lastOrderId = static_cast<int>(Time);
		writeBybitLastOrderId();
	}
}

/// Zorro identifies a trade by the id returned from BrokerBuy2, the exchange needs the symbol for every subsequent
/// operation. The mapping is persisted so that it survives a Zorro restart.
struct OpenTrade {
	std::string asset;

	/// Lots as REQUESTED from Zorro. It is an upper bound on what the entry order can ever fill, never the amount
	/// that actually filled - a partially filled order fills less, and that difference is what the exchange holds.
	int lots{0}; /// 0 for records written by older plugin versions

	/// Lots closed so far, counted cumulatively instead of subtracted from "lots". Subtracting would mix the
	/// requested size with the filled one: a request for 10 that filled 3 and was then fully closed would still
	/// leave 7 "open" in the record while the exchange position is flat.
	int closed{0};

	/// Client ids of closing orders whose outcome could not be established. Closes are sent IOC so they never rest
	/// on the book, but a lost response still leaves a fill that has to be booked - BrokerTrade retries these.
	std::vector<int> pendingCloses;
};

/// Ids of recently closed trades, so that BrokerTrade can tell "fully closed" (-1) apart from "unknown" (NAY)
static constexpr std::size_t MAX_CLOSED_TRADES = 200;

struct TradeStore {
	std::map<int, OpenTrade> open;
	std::vector<int> closed; /// Oldest first, capped at MAX_CLOSED_TRADES
};

TradeStore loadTrades() {
	TradeStore store;

	try {
		std::ifstream ifs(OPEN_TRADES_FILE);

		if (!ifs.is_open()) {
			return store;
		}

		const nlohmann::json json = nlohmann::json::parse(ifs, nullptr, false);

		if (json.is_discarded()) {
			spdlog::error("Malformed json file, path: {}, {}", OPEN_TRADES_FILE, MAKE_FILELINE);
			return store;
		}

		if (const auto it = json.find("openTrades"); it != json.end() && it->is_object()) {
			for (const auto &[key, value]: it->items()) {
				OpenTrade openTrade;

				if (value.is_string()) {
					/// Legacy format - the symbol was stored alone, the open size is unknown
					openTrade.asset = value.get<std::string>();
				} else if (value.is_object()) {
					if (const auto assetIt = value.find("asset"); assetIt != value.end()) {
						openTrade.asset = assetIt->get<std::string>();
					}

					if (const auto lotsIt = value.find("lots"); lotsIt != value.end()) {
						openTrade.lots = lotsIt->get<int>();
					}

					if (const auto closedIt = value.find("closed"); closedIt != value.end()) {
						openTrade.closed = closedIt->get<int>();
					}

					if (const auto pendingIt = value.find("pendingCloses");
						pendingIt != value.end() && pendingIt->is_array()) {
						openTrade.pendingCloses = pendingIt->get<std::vector<int> >();
					}
				}

				if (!openTrade.asset.empty()) {
					store.open.insert_or_assign(std::stoi(key), openTrade);
				}
			}
		}

		if (const auto it = json.find("closedTrades"); it != json.end() && it->is_array()) {
			store.closed = it->get<std::vector<int> >();
		}
	} catch (std::exception &e) {
		spdlog::error("{}: {}", MAKE_FILELINE, e.what());
	}

	return store;
}

/**
 * Write the store out. Opening the target file directly would truncate it first, so a crash or a full disk in the
 * middle of the write leaves an empty or half written file behind and the trade to symbol mapping is lost. Write to
 * a temporary file, flush it and rename it over the target, which is atomic on both Windows and POSIX.
 * @return false when the store could not be persisted
 */
bool saveTrades(const TradeStore &store) {
	const std::filesystem::path target(OPEN_TRADES_FILE);
	const auto temporary = std::filesystem::path(target).concat(".tmp");

	try {
		auto tradesJson = nlohmann::json::object();

		for (const auto &[tradeId, openTrade]: store.open) {
			tradesJson[std::to_string(tradeId)] = {
				{"asset", openTrade.asset}, {"lots", openTrade.lots}, {"closed", openTrade.closed},
				{"pendingCloses", openTrade.pendingCloses}
			};
		}

		nlohmann::json json;
		json["openTrades"] = tradesJson;
		json["closedTrades"] = store.closed; {
			std::ofstream ofs(temporary, std::ios::binary | std::ios::trunc);

			if (!ofs.is_open()) {
				spdlog::error("Couldn't save json file, path: {}, {}", temporary.string(), MAKE_FILELINE);
				return false;
			}

			ofs << json.dump(4);
			ofs.flush();

			if (!ofs.good()) {
				spdlog::error("Couldn't write json file, path: {}, {}", temporary.string(), MAKE_FILELINE);
				return false;
			}
		}

		std::filesystem::rename(temporary, target);
		return true;
	} catch (std::exception &e) {
		spdlog::error("{}: {}", MAKE_FILELINE, e.what());

		std::error_code ignored;
		std::filesystem::remove(temporary, ignored);
		return false;
	}
}

/// BrokerTrade is polled per open trade, so the store is kept in memory and only written through on a change
static std::optional<TradeStore> tradeStoreCache;

TradeStore &tradeStore() {
	if (!tradeStoreCache) {
		tradeStoreCache = loadTrades();
	}

	return *tradeStoreCache;
}

std::optional<OpenTrade> findOpenTrade(const int tradeId) {
	const auto &store = tradeStore();

	if (const auto it = store.open.find(tradeId); it != store.open.end()) {
		return it->second;
	}

	spdlog::error("Could not find Asset for trade id: {}, {}", tradeId, MAKE_FILELINE);
	return {};
}

bool wasTradeClosed(const int tradeId) {
	const auto &store = tradeStore();
	return std::ranges::find(store.closed, tradeId) != store.closed.end();
}

bool storeOpenTrade(const int tradeId, const std::string &asset, const int lots) {
	auto &store = tradeStore();
	store.open.insert_or_assign(tradeId, OpenTrade{asset, lots});
	return saveTrades(store);
}

/// Move a finished trade to the capped list of closed ids, which keeps the file from growing and lets BrokerTrade
/// report a closed trade instead of an unknown one
void markTradeClosed(TradeStore &store, const std::map<int, OpenTrade>::iterator &it) {
	const auto tradeId = it->first;
	store.open.erase(it);
	store.closed.push_back(tradeId);

	if (store.closed.size() > MAX_CLOSED_TRADES) {
		store.closed.erase(store.closed.begin(),
		                   store.closed.begin() + static_cast<long>(store.closed.size() - MAX_CLOSED_TRADES));
	}
}

/**
 * Account for lots that have just been closed. The record is retired once the closed amount reaches what was
 * requested; a trade whose entry filled only partially is retired by BrokerTrade, which is the first place that
 * knows the real entry fill.
 * @param tradeId
 * @param closedLots absolute number of closed lots
 */
void bookClosedLots(const int tradeId, const int closedLots) {
	auto &store = tradeStore();
	const auto it = store.open.find(tradeId);

	if (it == store.open.end()) {
		return;
	}

	it->second.closed += std::abs(closedLots);

	if (it->second.lots != 0 && it->second.closed >= std::abs(it->second.lots)) {
		markTradeClosed(store, it);
	}

	saveTrades(store);
}

/// Remember a closing order whose outcome could not be established, so that BrokerTrade can finish the job
void rememberPendingClose(const int tradeId, const int closeOrderId) {
	auto &store = tradeStore();

	if (const auto it = store.open.find(tradeId); it != store.open.end()) {
		it->second.pendingCloses.push_back(closeOrderId);
		saveTrades(store);
	}
}

/// Drop a closing order from the pending list once its fill has been booked
void forgetPendingClose(const int tradeId, const int closeOrderId) {
	auto &store = tradeStore();

	if (const auto it = store.open.find(tradeId); it != store.open.end()) {
		auto &pending = it->second.pendingCloses;
		std::erase(pending, closeOrderId);
		saveTrades(store);
	}
}

/// Retire a trade whose entry fill turned out to be fully closed already
void retireOpenTrade(const int tradeId) {
	auto &store = tradeStore();

	if (const auto it = store.open.find(tradeId); it != store.open.end()) {
		markTradeClosed(store, it);
		saveTrades(store);
	}
}

DLLFUNC_C int BrokerLogin(char *User, char *Pwd, char *Type, char *Account) {
	if (!User) {
		stopSymbolsUpdater();
		/// The stream manager holds a weak reference to the REST client, release it first
		streamManager.reset();
		restClient.reset();
		/// Drop the cached trade store, it is re-read from disk on the next login
		tradeStoreCache.reset();
		spdlog::info("Logout");
		spdlog::shutdown();
		return 1;
	}
	if (static_cast<std::string>(Type) == "Demo") {
		const auto msg = "Demo mode not supported by this plugin.";
		spdlog::error("{}: {}", MAKE_FILELINE, msg);
		BrokerError(msg);
		return 0;
	}
	if (!restClient) {
		const auto logger = spdlog::basic_logger_mt("bybit_logger", R"(./Log/bybit_futures.log)");
		spdlog::set_default_logger(logger);
		spdlog::flush_on(spdlog::level::info);
		logger->set_pattern("%+", spdlog::pattern_time_type::utc);

		if (!std::string_view(User).empty() && !std::string_view(Pwd).empty()) {
			restClient = std::make_shared<RESTClient>(User, Pwd);
			startSymbolsUpdater();
			readBybitLastOrderId();

			spdlog::info("Logged into account: " + std::string(Account));
			const std::string msg = "Plugin version: " + std::string(PLUGIN_VERSION_STR) + ",  release date: " +
			                  std::string(PLUGIN_VERSION_RELEASE_DATE);
			spdlog::info("Plugin version: " + msg);
			BrokerError(msg.c_str());
		} else {
			const auto msg = "Missing or Incomplete Account credentials.";
			spdlog::error("{}: {}", MAKE_FILELINE, msg);
			BrokerError(msg);
			return 0;
		}
	} else {
		restClient->setCredentials(User, Pwd);
	}

	try {
		if (!streamManager) {
			streamManager = std::make_unique<WSStreamManager>();
			streamManager->setLoggerCallback(&logFunction);
			streamManager->setRestClient(restClient);
			streamManager->setTimeout(STREAM_READ_TIMEOUT_S);
			streamManager->setMaxTickAge(MAX_TICK_AGE_S);
		}
	} catch (std::exception &e) {
		spdlog::error("{}: {}", MAKE_FILELINE, e.what());
		return 0;
	}

	try {
		const auto balances = restClient->getWalletBalance(AccountType::UNIFIED);
		return 1;
	} catch (std::exception &e) {
		spdlog::error("{}: {}", MAKE_FILELINE, e.what());
		return 0;
	}
}

DLLFUNC_C int
BrokerAsset(char *Asset, double *pPrice, double *pSpread, double *pVolume, double *pPip, double *pPipCost,
            double *pLotAmount, double *pMarginCost, double *pRollLong, double *pRollShort) {
	/// NOTE: Do not log normal state, this function is called ver often!
	if (!Asset || !*Asset) {
		return 0;
	}

	if (!restClient || !streamManager) {
		spdlog::critical("{}: {}", MAKE_FILELINE, "Bybit WS stream manager instance not initialized.");
		return 0;
	}

	/// Zorro subscribes an asset by calling with pPrice == NULL and only afterwards asks for prices
	const bool subscribing = pPrice == nullptr;

	if (pPip != nullptr || subscribing) {
		/// Contract parameters of the asset
		try {
			const auto instrument = restClient->getInstrumentInfo(Category::linear, Asset);

			if (!instrument) {
				const auto msg = "Unknown asset: " + std::string(Asset);
				spdlog::error("{}: {}", MAKE_FILELINE, msg);
				BrokerError(msg.c_str());
				return 0;
			}

			const auto tickSize = instrument->priceFilter.tickSize;
			const auto qtyStep = instrument->lotSizeFilter.qtyStep;

			if (tickSize <= 0.0 || qtyStep <= 0.0) {
				const auto msg = "Incomplete asset info for: " + std::string(Asset);
				spdlog::error("{}: {}", MAKE_FILELINE, msg);
				BrokerError(msg.c_str());
				return 0;
			}

			if (pPip) {
				*pPip = tickSize;
			}

			if (pLotAmount) {
				*pLotAmount = qtyStep;
			}

			if (pPipCost) {
				*pPipCost = tickSize * qtyStep;
			}

#ifdef _WIN64
			lotAmounts.insert_or_assign(Asset, qtyStep);
#endif
		} catch (std::exception &e) {
			spdlog::error("{}: {}\n", MAKE_FILELINE, e.what());
			BrokerError("Cannot acquire asset info from server.");
			return 0;
		}
	}

	try {
		/// Check if the Ticker Stream is subscribed for the Asset
		streamManager->subscribeTickerStream(Asset);

		if (subscribing) {
			/// Subscription call - the asset exists and its stream is running, that is all Zorro asks for here.
			/// It must NOT depend on a quote having arrived: an asset that returns 0 after subscription triggers
			/// Error 053 and gets its trading disabled, while an illiquid symbol can stay silent for minutes.
			return 1;
		}

		/// Reading falls back to a REST snapshot when the stream stays silent, which is the normal case for an
		/// illiquid symbol - its top of book simply does not change for minutes.
		if (const auto instrumentInfo = streamManager->readEventTicker(Asset)) {
			const auto &info = *instrumentInfo;

			if (info.ask1Price <= 0.0 || info.bid1Price <= 0.0) {
				const auto msg = "Invalid bid/ask for Asset: " + std::string(Asset);
				spdlog::error("{}: {}", MAKE_FILELINE, msg);
				return 0;
			}

			/// Zorro expects the ask price here, the bid is derived from it via the spread
			if (pPrice) {
				*pPrice = info.ask1Price;
			}
			if (pSpread) {
				*pSpread = info.ask1Price - info.bid1Price;
			}
			if (pVolume) {
				/// The ticker stream carries no traded volume, the top of book depth is the closest proxy
				*pVolume = info.ask1Size + info.bid1Size;
			}

			return 1;
		}
		const auto msg = "Could not read Ticker Stream for Asset: " + std::string(Asset) + ", reading timeout";
		spdlog::error("{}: {}", MAKE_FILELINE, msg);
	} catch (std::exception &e) {
		spdlog::error("{}: {}", MAKE_FILELINE, e.what());
		BrokerError("Cannot acquire asset info from server.");
	}

	return 0;
}

DLLFUNC_C int BrokerAccount(char *Account, double *pdBalance, double *pdTradeVal, double *pdMarginVal) {
	if (!restClient) {
		spdlog::critical("{}: {}", MAKE_FILELINE, "Bybit Rest Client instance not initialized.");
		return 0;
	}

	if (!Account || !*Account) {
		accountCurrency = "USDT";
	} else {
		accountCurrency = Account;
	}

	try {
		const auto accountBalances = restClient->getWalletBalance(AccountType::UNIFIED);

		double totalBalance = 0.0;
		double totalUnrealisedPnl = 0.0;
		double totalMargin = 0.0;
		bool coinFound = false;

		for (const auto &el: accountBalances.balances) {
			for (const auto &elCoin: el.coins) {
				if (elCoin.coin != accountCurrency) {
					continue;
				}

				coinFound = true;
				totalBalance += elCoin.walletBalance;
				totalUnrealisedPnl += elCoin.unrealisedPnl;
				totalMargin += elCoin.totalPositionIM + elCoin.totalOrderIM;
			}
		}

		if (!coinFound) {
			const auto msg = "Account currency not found: " + accountCurrency;
			spdlog::error("{}: {}", MAKE_FILELINE, msg);
			BrokerError(msg.c_str());
			return 0;
		}

		if (pdBalance) {
			*pdBalance = totalBalance;
		}

		/// Value of open positions and the margin they bind - without those Zorro cannot track equity
		if (pdTradeVal) {
			*pdTradeVal = totalUnrealisedPnl;
		}

		if (pdMarginVal) {
			*pdMarginVal = totalMargin;
		}

		return 1;
	} catch (std::exception &e) {
		spdlog::error("{}: {}", MAKE_FILELINE, e.what());
		BrokerError("Cannot acquire wallet balance from server.");
	}

	return 0;
}

DLLFUNC_C int BrokerTime(DATE *pTimeGMT) {
	if (!restClient) {
		spdlog::critical("{}: {}", MAKE_FILELINE, "Bybit Rest Client instance not initialized.");
		return ExchangeStatus::Unavailable;
	}

	/// Zorro uses this to notice that the connection broke down, to stop trading and to keep probing until it is
	/// back. Reporting the market as open just because the client object exists hides every outage.
	static std::int64_t lastProbeMs = 0;
	static bool connectionOk = true;

	const auto nowMs = stonky::getMsTimestamp(stonky::currentTime()).count();

	/// Any recent successful response is proof enough, no need to spend a request on it
	if (const auto lastSuccessMs = restClient->lastSuccessfulResponseMs();
		lastSuccessMs > 0 && nowMs - lastSuccessMs < CONNECTION_FRESH_MS) {
		connectionOk = true;
		/// Bybit never closes
		return ExchangeStatus::Open;
	}

	if (nowMs - lastProbeMs < CONNECTION_PROBE_INTERVAL_MS) {
		return connectionOk ? ExchangeStatus::Open : ExchangeStatus::Unavailable;
	}

	lastProbeMs = nowMs;

	try {
		const auto timeInMs = restClient->getServerTime();

		if (pTimeGMT) {
			*pTimeGMT = convertTime(timeInMs);
		}

		connectionOk = true;
		return ExchangeStatus::Open;
	} catch (std::exception &e) {
		spdlog::error("{}: connection probe failed: {}", MAKE_FILELINE, e.what());
		connectionOk = false;
		return ExchangeStatus::Unavailable;
	}
}

DLLFUNC_C int BrokerHistory2(char *Asset, DATE tStart, DATE tEnd, int nTickMinutes, int nTicks, T6 *ticks) {
	if (!Asset || !ticks || !nTicks) {
		return 0;
	}

	if (!restClient) {
		spdlog::critical("{}: {}", MAKE_FILELINE, "Bybit Rest Client instance not initialized.");
		return 0;
	}

	try {
		auto candleInterval = CandleInterval::_1;

		if (!Bybit::isValidCandleResolution(nTickMinutes, candleInterval)) {
			std::string msg = "Invalid data resolution: " + std::to_string(nTickMinutes) + " minutes.";
			spdlog::error("{}: {}", MAKE_FILELINE, msg);
			BrokerError(msg.c_str());
			return 0;
		}

		const auto msInInterval = Bybit::numberOfMsForCandleInterval(candleInterval);
		auto candles = restClient->getHistoricalPrices(Category::linear, Asset, candleInterval,
		                                               convertTime(tStart) - msInInterval,
		                                               convertTime(tEnd) - msInInterval);

		if (candles.empty()) {
			std::string msg = "No historical data.";
			spdlog::error("{}: {}", MAKE_FILELINE, msg);
			BrokerError(msg.c_str());
			return 0;
		}

		const auto maxCandles = std::min(nTicks, (int) (candles).size());
		std::ranges::reverse(candles);

		/// From most recent to oldest.
		for (int i = 0; i < maxCandles; i++, ticks++) {
			ticks->fOpen = static_cast<float>(candles[i].open);
			ticks->fHigh = static_cast<float>(candles[i].high);
			ticks->fLow = static_cast<float>(candles[i].low);
			ticks->fClose = static_cast<float>(candles[i].close);
			ticks->fVol = static_cast<float>(candles[i].volume);

			/// Zorro uses reversed order in time series so that's why...
			ticks->time = convertTime(candles[i].startTime + msInInterval);
		}

		return maxCandles;
	} catch (std::exception &e) {
		spdlog::error("{}: {}", MAKE_FILELINE, e.what());
		BrokerError("Cannot acquire historical data from server.");
	}

	return 0;
}

/// True once the exchange has made up its mind about the order - anything else means "ask again"
bool isSettledOrderStatus(const OrderStatus status) {
	switch (status) {
		case OrderStatus::New:
		case OrderStatus::PartiallyFilled:
		case OrderStatus::PartiallyFilledCanceled:
		case OrderStatus::Filled:
		case OrderStatus::Cancelled:
		case OrderStatus::Rejected:
		case OrderStatus::Deactivated:
		case OrderStatus::Active:
			return true;
		default:
			/// Created, PendingCancel, Untriggered, Triggered
			return false;
	}
}

/// The exchange will not change this order anymore. Distinct from isSettledOrderStatus, which also counts a resting
/// order as decided.
bool isTerminalOrderStatus(const OrderStatus status) {
	switch (status) {
		case OrderStatus::Filled:
		case OrderStatus::Cancelled:
		case OrderStatus::Rejected:
		case OrderStatus::Deactivated:
		case OrderStatus::PartiallyFilledCanceled:
			return true;
		default:
			return false;
	}
}

/// A resting order (New) is a valid outcome, not a failure - it must be reported to Zorro as a pending trade
bool isLiveOrderStatus(const OrderStatus status) {
	return status == OrderStatus::New || status == OrderStatus::PartiallyFilled || status == OrderStatus::Filled ||
	       status == OrderStatus::Active;
}

/**
 * Bybit's order create response carries no execution state, it has to be polled. Returns the last state seen, or
 * bad option when the order could not be resolved within the attempt budget.
 */
std::optional<OrderResponse> pollOrderState(const std::string &symbol, const OrderId &orderId) {
	std::optional<OrderResponse> lastState;

	for (int attempt = 0; attempt < MAX_ORDER_POLL_ATTEMPTS; attempt++) {
		if (const auto activeOrder = restClient->getOpenOrder(Category::linear, symbol, orderId.orderId,
		                                                      orderId.orderLinkId)) {
			lastState = activeOrder;

			if (isSettledOrderStatus(activeOrder->orderStatus)) {
				return lastState;
			}
		}

		std::this_thread::sleep_for(ORDER_POLL_INTERVAL);
	}

	return lastState;
}

/**
 * Establish what really happened to an order whose send failed with an unknown outcome. Bybit may well have
 * accepted it, so it must never be reported as a plain rejection.
 *
 * Zorro requires the plugin to cancel an order it answers with -2, so an order still resting on the book is
 * cancelled here and the resulting fill is taken as the truth.
 *
 * @return the trade id when the order holds a position, 0 when it never filled, -2 when even the reconciliation
 *         could not reach the exchange
 */
int reconcileUnknownOrder(const std::string &asset, const int tradeId, const int amount, double *pPrice,
                          int *pFill);

/**
 * Drive an order to a state the exchange will not change anymore and report what it filled.
 *
 * Bybit's cancel is asynchronous - the response only means the request was accepted - so a single query after it can
 * still answer New. Taking that for "did not fill" would report a live order as rejected. Poll until it settles.
 *
 * @param cancelIfLive send a cancel when the order is still working. Zorro requires this before -2 is returned.
 * @return filled lots once the order is settled, bad option when that could not be established
 */
std::optional<int> resolveOrderOutcome(const std::string &asset, const int clientOrderId, const bool cancelIfLive) {
	const auto orderLinkId = std::to_string(clientOrderId);
	bool cancelSent = false;

	for (int attempt = 0; attempt < MAX_ORDER_POLL_ATTEMPTS; attempt++) {
		std::optional<OrderResponse> order;

		try {
			order = restClient->getOpenOrder(Category::linear, asset, "", orderLinkId);
		} catch (const OrderNotFound &) {
			/// The exchange settles the question: it never got the order
			return 0;
		} catch (const std::exception &e) {
			/// Anything else - transport, 5xx, auth, rate limit - says nothing about the order
			spdlog::warn("{}: query of {} failed: {}", MAKE_FILELINE, orderLinkId, e.what());
			std::this_thread::sleep_for(ORDER_POLL_INTERVAL);
			continue;
		}

		if (!order) {
			/// Gone from the realtime endpoint: either finished or never placed. The fills are the remaining truth.
			try {
				double filledQty = 0.0;

				for (const auto &execution: restClient->getExecutions(Category::linear, asset, orderLinkId)) {
					if (execution.execType == ExecType::Trade) {
						filledQty += execution.execQty;
					}
				}

				return static_cast<int>(std::round(filledQty / lotAmount));
			} catch (const std::exception &e) {
				spdlog::warn("{}: executions of {} failed: {}", MAKE_FILELINE, orderLinkId, e.what());
				std::this_thread::sleep_for(ORDER_POLL_INTERVAL);
				continue;
			}
		}

		if (isTerminalOrderStatus(order->orderStatus)) {
			return static_cast<int>(std::round(order->cumExecQty / lotAmount));
		}

		if (cancelIfLive && !cancelSent) {
			cancelSent = true;
			spdlog::warn("{}: order {} is still working, cancelling it", MAKE_FILELINE, orderLinkId);

			try {
				static_cast<void>(restClient->cancelOrder(Category::linear, asset, "", orderLinkId));
			} catch (std::exception &e) {
				/// It may have filled in the meantime, the polling decides
				spdlog::warn("{}: cancel of {} failed: {}", MAKE_FILELINE, orderLinkId, e.what());
			}
		}

		std::this_thread::sleep_for(ORDER_POLL_INTERVAL);
	}

	spdlog::error("{}: order {} did not settle, its outcome stays unknown", MAKE_FILELINE, orderLinkId);
	return {};
}

/**
 * Establish what really happened to an order whose outcome is unknown. Bybit may well have accepted it, so it must
 * never be reported as a plain rejection. An order still resting is cancelled, which Zorro requires before -2.
 *
 * @return the trade id when the order holds a position, 0 when it never filled, -2 when it could not be settled
 */
int reconcileUnknownOrder(const std::string &asset, const int tradeId, const int amount, double *pPrice,
                          int *pFill) {
	const auto filledLots = resolveOrderOutcome(asset, tradeId, true);

	if (!filledLots) {
		BrokerError("Order state unknown, check the exchange for an orphaned order.");
		return -2;
	}

	if (*filledLots <= 0) {
		spdlog::info("Order " + std::to_string(tradeId) + " did not fill, nothing was opened");
		return 0;
	}

	if (pFill) {
		*pFill = *filledLots;
	}

	spdlog::warn("{}: order {} was accepted despite the unknown outcome, filled size: {}", MAKE_FILELINE, tradeId,
	             *filledLots);

	if (!storeOpenTrade(tradeId, asset, amount)) {
		BrokerError("Order reconciled but the trade record could not be saved, see the log.");
	}

	return tradeId;
}

DLLFUNC_C int BrokerBuy2(char *Asset, int Amount, double dStopDist, double Limit, double *pPrice, int *pFill) {
	if (!restClient) {
		spdlog::critical("{}: {}", MAKE_FILELINE, "Bybit Rest Client instance not initialized.");
		return 0;
	}

	if (!Asset || !*Asset || Amount == 0) {
		return 0;
	}

	/// Known before the order goes out, so that an unknown outcome can still be reconciled by order link id
	int tradeId = 0;

	try {
#ifdef _WIN64
		if (const auto it = lotAmounts.find(Asset); it != lotAmounts.end()) {
			lotAmount = it->second;
		} else {
			std::string msg = "Cannot find lot amount size for asset: " + std::string(Asset);
			spdlog::error("{}: {}", MAKE_FILELINE, msg);
			return 0;
		}
#endif
		spdlog::info("New Order for asset: " + std::string(Asset) + ", amount: " + std::to_string(Amount) + ", size: " +
		             std::to_string(lotAmount * std::abs(Amount)) + ", stop dist:" + std::to_string(Limit) +
		             ", limit: " +
		             std::to_string(Limit));

		Order order;
		order.symbol = Asset;

		if (Amount > 0) {
			order.side = Side::Buy;
		} else {
			order.side = Side::Sell;
		}

		if (orderType == 1) {
			order.timeInForce = TimeInForce::IOC;
		} else if (orderType == 2) {
			order.timeInForce = TimeInForce::GTC;
		} else {
			order.timeInForce = TimeInForce::FOK;
		}

		/// NOTE: dStopDist is deliberately ignored - this plugin does not attach a broker side stop to the order,
		/// so the stop loss stays with Zorro and is only executed while Zorro is running. See README.
		if (Limit > 0.) {
			order.price = Limit;
			order.orderType = OrderType::Limit;
		} else {
			order.orderType = OrderType::Market;
		}

		order.qty = lotAmount * std::abs(Amount);

		readBybitLastOrderId();
		order.orderLinkId = std::to_string(lastOrderId++);
		writeBybitLastOrderId();

		/// A non-zero positionIdx on the existing position record means the account is in Hedge mode; One-way mode
		/// requires positionIdx 0, which is also the safe default when no position record exists yet.
		const auto positionResponse = restClient->getPositionInfo(Category::linear, Asset);
		hedge = !positionResponse.empty() && positionResponse[0].positionIdx != 0;

		if (hedge) {
			order.positionIdx = order.side == Side::Buy ? 1 : 2;
		} else {
			order.positionIdx = 0;
		}

		tradeId = std::stoi(order.orderLinkId);
		const auto orderId = restClient->placeOrder(order);
		const auto orderState = pollOrderState(Asset, orderId);

		if (!orderState) {
			/// The order was accepted by placeOrder but its state never resolved - it may be live on the exchange
			spdlog::error("{}: {}", MAKE_FILELINE,
			              "Order state could not be resolved, order may be live on the exchange, orderLinkId: " +
			              order.orderLinkId);
			BrokerError("No order confirmation from server, order state unknown.");
			return -2;
		}

		const auto filledLots = static_cast<int>(std::round(orderState->cumExecQty / lotAmount));

		/// A GTC limit order normally comes back as New - it rests on the book and must NOT be reported as a
		/// failure, otherwise it stays open on the exchange while Zorro believes nothing happened. A partially
		/// filled IOC (PartiallyFilledCanceled) is a real position and must be reported as well.
		if (isLiveOrderStatus(orderState->orderStatus) || filledLots > 0) {
			if (pPrice && orderState->avgPrice > 0.0) {
				*pPrice = orderState->avgPrice;
			}

			if (pFill) {
				*pFill = filledLots;
			}

			spdlog::info("Order placed for asset: " + std::string(Asset) + ", status: " +
			             std::string(magic_enum::enum_name(orderState->orderStatus)) + ", filled size: " +
			             std::to_string(orderState->cumExecQty / lotAmount) + ", average price: " +
			             std::to_string(orderState->avgPrice) + ", clientId: " + order.orderLinkId);

			if (!storeOpenTrade(tradeId, Asset, Amount)) {
				/// The order is live on the exchange, so the trade id must still be returned - Zorro keeps its own
				/// record of it. But without the persisted mapping a restart could not close this trade, which the
				/// operator has to know about.
				BrokerError("Order placed but the trade record could not be saved, see the log.");
			}

			return tradeId;
		}

		if (!isSettledOrderStatus(orderState->orderStatus)) {
			/// Still Created/Untriggered after the whole poll budget - the order is not dead, its outcome is
			/// unknown. Reporting a rejection here would invite a duplicate order.
			spdlog::error("{}: {}", MAKE_FILELINE,
			              "Order not settled within the poll budget, state unknown, orderLinkId: " +
			              order.orderLinkId);
			BrokerError("No order confirmation from server, order state unknown.");
			return -2;
		}

		const std::string msg =
				"Cannot place order: " + std::string(Asset) + ", size: " + std::to_string(Amount) +
				", reason: " + std::string(magic_enum::enum_name(orderState->orderStatus)) +
				(orderState->rejectReason.empty() ? "" : ", " + orderState->rejectReason);
		spdlog::error("{}: {}", MAKE_FILELINE, msg);
		BrokerError(msg.c_str());
	} catch (const UnknownOutcomeError &e) {
		/// The order may well have reached the exchange - reporting a rejection here would risk a duplicate order.
		spdlog::error("{}: {}", MAKE_FILELINE, e.what());

		if (tradeId == 0) {
			/// It never got as far as being sent
			BrokerError("Cannot send order to server.");
			return 0;
		}

		return reconcileUnknownOrder(Asset, tradeId, Amount, pPrice, pFill);
	} catch (std::exception &e) {
		spdlog::error("{}: {}", MAKE_FILELINE, e.what());
		BrokerError("Cannot send order to server.");
	}

	return 0;
}

DLLFUNC_C int
BrokerSell2(int nTradeId, int nAmount, double Limit, double *pClose, double *pCost, double *pProfit, int *pFill) {
	if (!restClient) {
		spdlog::critical("{}: {}", MAKE_FILELINE, "Bybit Rest Client instance not initialized.");
		return 0;
	}

	if (nAmount == 0) {
		return 0;
	}

	try {
		/// Do NOT drop the mapping here - if the closing order fails the trade would become impossible to close
		const auto openTrade = findOpenTrade(nTradeId);

		if (!openTrade) {
			return 0;
		}

		const auto asset = openTrade->asset;

#ifdef _WIN64
		auto it = lotAmounts.find(asset);
		if (it != lotAmounts.end()) {
			lotAmount = it->second;
		} else {
			std::string msg = "Cannot find lot amount size for asset: " + std::string(asset);
			spdlog::error("{}: {}", MAKE_FILELINE, msg);
			return 0;
		}
#endif
		Order order;
		order.symbol = asset;

		if (nAmount > 0) {
			order.side = Side::Sell;
		} else {
			order.side = Side::Buy;
		}

		if (Limit > 0.) {
			order.price = Limit;
			order.orderType = OrderType::Limit;
		} else {
			order.orderType = OrderType::Market;
		}

		order.qty = lotAmount * std::abs(nAmount);

		readBybitLastOrderId();
		order.orderLinkId = std::to_string(lastOrderId++);
		writeBybitLastOrderId();

		/// A close must never rest on the book. Nothing tracks it afterwards, so an order that fills later would
		/// leave Zorro holding a trade that no longer exists on the exchange. IOC fills what it can right away and
		/// cancels the rest; a market close is immediate anyway.
		order.timeInForce = TimeInForce::IOC;

		/// Hedge mode closes the leg identified by positionIdx, One-way mode expresses the closing intent through
		/// reduceOnly, which also prevents an oversized close from flipping the position
		const auto positionResponse = restClient->getPositionInfo(Category::linear, asset);
		hedge = !positionResponse.empty() && positionResponse[0].positionIdx != 0;

		if (hedge) {
			order.positionIdx = order.side == Side::Buy ? 2 : 1;
		} else {
			order.positionIdx = 0;
			order.reduceOnly = true;
		}

		const auto closeOrderId = std::stoi(order.orderLinkId);
		std::optional<int> filledLots;
		double closePrice = 0.0;

		try {
			const auto orderId = restClient->placeOrder(order);

			if (const auto orderState = pollOrderState(asset, orderId);
				orderState && isTerminalOrderStatus(orderState->orderStatus)) {
				filledLots = static_cast<int>(std::round(orderState->cumExecQty / lotAmount));
				closePrice = orderState->avgPrice;
			} else {
				/// Not terminal within the poll budget - settle it instead of guessing
				filledLots = resolveOrderOutcome(asset, closeOrderId, true);
			}
		} catch (const UnknownOutcomeError &e) {
			/// The close may well have executed - find out instead of guessing
			spdlog::error("{}: closing order not confirmed, reconciling: {}", MAKE_FILELINE, e.what());
			filledLots = resolveOrderOutcome(asset, closeOrderId, true);
		}

		if (!filledLots) {
			/// The outcome stays unknown. Remember the close order so that BrokerTrade books its fill later, and
			/// report the trade as not closed - Zorro retries, and reduceOnly keeps a repeat from flipping the
			/// position.
			rememberPendingClose(nTradeId, closeOrderId);
			BrokerError("Close state unknown, it will be reconciled, see the log.");
			return 0;
		}

		if (*filledLots > 0) {
			if (pFill) {
				*pFill = *filledLots;
			}

			if (pClose && closePrice > 0.0) {
				*pClose = closePrice;
			}

			spdlog::info("Closing order for asset: " + std::string(asset) + ", filled size: " +
			             std::to_string(*filledLots) + ", average price: " + std::to_string(closePrice) +
			             ", clientId: " + order.orderLinkId);

			/// Book the closed lots against the record, it is retired once nothing is left open
			bookClosedLots(nTradeId, *filledLots);

			/// Zorro keeps addressing the remainder by the original id
			return nTradeId;
		}

		const std::string msg = "Closing order did not fill: " + std::string(asset) + ", size: " +
		                        std::to_string(nAmount);
		spdlog::error("{}: {}", MAKE_FILELINE, msg);
		BrokerError(msg.c_str());
	} catch (std::exception &e) {
		spdlog::error("{}: {}", MAKE_FILELINE, e.what());
		BrokerError("Cannot close trade.");
	}

	return 0;
}

/**
 * Report the fill state of an order/trade back to Zorro. Without it Zorro cannot tell whether a resting limit order
 * has been filled in the meantime.
 *
 * Return value (Zorro broker API): current fill amount in lots as in BrokerBuy2, -1 when the trade was completely
 * closed, NAY when the state is unavailable, NAY-1 when the order was cancelled or removed by the broker.
 */
DLLFUNC_C int BrokerTrade(int nTradeId, double *pOpen, double *pClose, double *pCost, double *pProfit) {
	if (!restClient) {
		spdlog::critical("{}: {}", MAKE_FILELINE, "Bybit Rest Client instance not initialized.");
		return NAY;
	}

	const auto openTrade = findOpenTrade(nTradeId);

	if (!openTrade) {
		/// Nothing open under this id - either it was closed through this plugin, or it is simply not known here
		return wasTradeClosed(nTradeId) ? -1 : NAY;
	}

	try {
#ifdef _WIN64
		if (const auto it = lotAmounts.find(openTrade->asset); it != lotAmounts.end()) {
			lotAmount = it->second;
		} else {
			spdlog::error("{}: Cannot find lot amount size for asset: {}", MAKE_FILELINE, openTrade->asset);
			return NAY;
		}
#endif
		/// A close whose outcome was never established still holds a fill that has to be booked, otherwise this
		/// trade would keep reporting lots that the exchange no longer has
		for (const auto closeOrderId: openTrade->pendingCloses) {
			if (const auto closedLots = resolveOrderOutcome(openTrade->asset, closeOrderId, true)) {
				spdlog::info("Pending close " + std::to_string(closeOrderId) + " settled, filled size: " +
				             std::to_string(*closedLots));

				if (*closedLots > 0) {
					bookClosedLots(nTradeId, *closedLots);
				}

				forgetPendingClose(nTradeId, closeOrderId);
			}
		}

		/// bookClosedLots may have retired the trade, so re-read the record
		const auto currentTrade = findOpenTrade(nTradeId);

		if (!currentTrade) {
			return wasTradeClosed(nTradeId) ? -1 : NAY;
		}

		const auto orderLinkId = std::to_string(nTradeId);
		double filledQty = 0.0;
		double avgPrice = 0.0;
		bool orderGone = false;

		if (const auto order = restClient->getOpenOrder(Category::linear, currentTrade->asset, "", orderLinkId)) {
			filledQty = order->cumExecQty;
			avgPrice = order->avgPrice;
			/// Cancelled, Rejected, Deactivated, Filled - the exchange will not add to it anymore
			orderGone = isTerminalOrderStatus(order->orderStatus);
		} else {
			/// Bybit's realtime order endpoint only serves orders that are still open. A fully filled order is
			/// gone from it, so the fills themselves are the remaining source of truth.
			double execValue = 0.0;

			for (const auto &execution: restClient->getExecutions(Category::linear, currentTrade->asset,
			                                                     orderLinkId)) {
				if (execution.execType != ExecType::Trade) {
					/// Funding and the like share this topic, they are not fills
					continue;
				}

				filledQty += execution.execQty;
				execValue += execution.execValue;
			}

			orderGone = true;

			if (filledQty > 0.0) {
				avgPrice = execValue / filledQty;
			}
		}

		if (pOpen && avgPrice > 0.0) {
			*pOpen = avgPrice;
		}

		const auto filledLots = static_cast<int>(std::round(filledQty / lotAmount));

		/// What the entry order actually put on the exchange, minus what has been closed since. The requested size
		/// only caps it - an order for 10 that filled 3 opened 3 lots, not 10.
		const auto entryLots = currentTrade->lots != 0
			                       ? std::min(std::abs(currentTrade->lots), filledLots)
			                       : filledLots;

		if (const auto openLots = entryLots - currentTrade->closed; openLots > 0) {
			return openLots;
		}

		if (entryLots > 0 || orderGone) {
			/// Everything the entry filled has been closed again, or the order is gone without a fill
			retireOpenTrade(nTradeId);
			return entryLots > 0 ? -1 : NAY - 1;
		}

		/// Still resting on the book, nothing filled yet
		return 0;
	} catch (std::exception &e) {
		spdlog::error("{}: {}", MAKE_FILELINE, e.what());
	}

	return NAY;
}

DLLFUNC_C double BrokerCommand(int Command, DWORD dwParameter) {
	switch (Command) {
		case SET_ORDERTYPE:
			return orderType = dwParameter;
		case SET_DELAY:
			loopMs = dwParameter;
		case GET_DELAY:
			return loopMs;
		case SET_AMOUNT:
#ifndef _WIN64
			lotAmount = *(double *) dwParameter;
#endif
			return 1;
		case SET_WAIT:
			waitMs = dwParameter;

			/// Drives how long a price read may block before falling back to the REST snapshot. Clamped so that a
			/// generous Zorro setting cannot stall the whole asset loop.
			if (streamManager) {
				streamManager->setTimeout(std::clamp(waitMs / 1000, 1, STREAM_READ_TIMEOUT_S));
			}

			/// Zorro's wait time is what the broker is allowed to take, so bound the REST requests by it too
			if (restClient) {
				restClient->setRequestTimeout(waitMs);
			}
		case GET_WAIT:
			return waitMs;
		case SET_SYMBOL:
			currentSymbol = (char *) dwParameter;
			return 1;
		case GET_POSITION:
			if (restClient) {
				const char *symbol = (char *) dwParameter;
				try {
					auto positions = restClient->getPositionInfo(Category::linear, symbol);

					double totalPositionAmt = 0;

					for (const auto &position: positions) {
						if (position.side == Side::Sell) {
							totalPositionAmt -= position.size;
						} else {
							totalPositionAmt += position.size;
						}
					}

					/// Zorro expects the net open amount in the same unit as BrokerBuy2, i.e. in lots, not in
					/// contracts. The lot size is per symbol, the global lotAmount belongs to the last order.
					double symbolLotAmount = lotAmount;
#ifdef _WIN64
					if (const auto it = lotAmounts.find(symbol); it != lotAmounts.end()) {
						symbolLotAmount = it->second;
					} else {
						spdlog::error("{}: Cannot find lot amount size for asset: {}", MAKE_FILELINE, symbol);
						return 0;
					}
#endif
					if (symbolLotAmount <= 0.0) {
						return 0;
					}

					return totalPositionAmt / symbolLotAmount;
				} catch (std::exception &e) {
					spdlog::error("{}: {}", MAKE_FILELINE, e.what());
					BrokerError((std::string("Cannot get position of " + std::string(symbol)).c_str()));
				}
			}
			break;
		case GET_COMPLIANCE:
			/// 2 = no hedging. In One-way mode the exchange nets an opposite order against the existing position,
			/// so Zorro must not open a counter trade and believe it holds two independent positions.
			return hedge ? 0 : 2;
		case GET_BROKERZONE:
			return 0; //return 0 for UTC
		case GET_MAXREQUESTS:
			return 10;
		case GET_MAXTICKS:
			return 250;
		//       case GET_COMPLIANCE:
		//           return 0;
		case DO_CANCEL:
			if (restClient) {
				try {
					const auto id = restClient->cancelOrder(Category::linear, currentSymbol, "",
					                                        std::to_string(dwParameter));
					spdlog::info("Order canceled for asset: " + std::string(currentSymbol) + ", order id: " +
					             id.orderLinkId);
					return 1;
				} catch (std::exception &e) {
					spdlog::error("{}: {}", MAKE_FILELINE, e.what());
					BrokerError((std::string(
						"Cannot cancel order id " + std::to_string(dwParameter)).c_str()));
				}
			}
			return 0;

		default:
			return 0;
	}

	return 0;
}

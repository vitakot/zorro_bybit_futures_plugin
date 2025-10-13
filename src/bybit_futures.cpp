/**
Bybit Futures Zorro Plugin

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@gmail.com>.
*/

#include "stdafx.h"
#include "vk/bybit/bybit_rest_client.h"
#include "vk/bybit/bybit_ws_stream_manager.h"
#include "vk/utils/utils.h"
#include "vk/utils/registry.h"
#include "bybit_futures.h"
#include <wtypes.h>
#include <string>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <algorithm>
#include <fstream>

#include "vk/bybit/bybit.h"

#define PLUGIN_VERSION    2
#define PLUGIN_VERSION_STR "1.0.0"
#define PLUGIN_VERSION_RELEASE_DATE "13-october-2025"
#undef min

#define ZORRO_REG_KEY "SOFTWARE\\Zorro"
#define LAST_ORDER_ID_KEY "BybitLastOrderId"
#define OPEN_TRADES_FILE R"(./Data/bybit_open_trades.json)"

using namespace std::chrono_literals;
using namespace vk::bybit;

static std::string currentSymbol;
static std::string accountCurrency;
static int lastOrderId = 0;
static int orderType = 0;
static double lotAmount = 1.0;
static int loopMs = 50; // Actually unused
static int waitMs = 30000; // Actually unused

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

void logFunction(const vk::LogSeverity severity, const std::string &errmsg) {
	switch (severity) {
		case vk::LogSeverity::Info:
			spdlog::info(errmsg);
			break;
		case vk::LogSeverity::Warning:
			spdlog::warn(errmsg);
			break;
		case vk::LogSeverity::Critical:
			spdlog::critical(errmsg);
			break;
		case vk::LogSeverity::Error:
			spdlog::error(errmsg);
			break;
		case vk::LogSeverity::Debug:
			spdlog::debug(errmsg);
			break;
		case vk::LogSeverity::Trace:
			spdlog::trace(errmsg);
			break;
	}
}

void symbolsUpdaterFunc() {
	symbolsUpdaterRunning = true;
	int numPass = 60;

	std::unique_ptr<RESTClient> restClientUpdater;

	while (symbolsUpdaterRunning) {
		if (numPass == 60) {
			numPass = 0;

			try {
				if (!restClientUpdater) {
					restClientUpdater = std::make_unique<RESTClient>("", "");
				}

				if (restClientUpdater && restClient) {
					restClient->setInstruments(restClientUpdater->getInstrumentsInfo(Category::linear, "", true));
				}
			} catch (std::exception &e) {
				spdlog::error("{}: {}", MAKE_FILELINE, e.what());
				spdlog::info("Resetting restClientUpdater, {}", MAKE_FILELINE);
				restClientUpdater = std::make_unique<RESTClient>("", "");
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
	if (const bool success = vk::writeInRegistry(HKEY_CURRENT_USER, ZORRO_REG_KEY, LAST_ORDER_ID_KEY, lastOrderId); !
		success) {
		spdlog::error("Cannot store BybitLastOrderId");
	}
}

void readBybitLastOrderId() {
	DWORD bybitLastOrderId;
	const bool success = vk::readDwordValueRegistry(HKEY_CURRENT_USER, ZORRO_REG_KEY, LAST_ORDER_ID_KEY,
	                                           &bybitLastOrderId);
	if (success) {
		lastOrderId = static_cast<int>(bybitLastOrderId);
	} else {
		time_t Time;
		time(&Time);
		lastOrderId = static_cast<int>(Time);
		writeBybitLastOrderId();
	}
}

std::string findAssetForTradeId(int tradeId, bool erase = true) {
	if (std::ifstream ifs(OPEN_TRADES_FILE); ifs.is_open()) {
		nlohmann::json json;
		json = nlohmann::json::parse(ifs);
		ifs.close();

		if (auto it = json.find("openTrades"); it != json.end()) {
			std::map<int, std::string> openTrades;
			openTrades = it->get<std::map<int, std::string> >();

			if (const auto tradeIt = openTrades.find(tradeId); tradeIt != openTrades.end()) {
				auto retVal = tradeIt->second;

				if (erase) {
					openTrades.erase(tradeIt);

					json["openTrades"] = openTrades;

					if (std::ofstream ofs(OPEN_TRADES_FILE); ofs.is_open()) {
						ofs << json.dump(4);
						ofs.close();
					} else {
						spdlog::error("Couldn't save json file, path: {}, {}", OPEN_TRADES_FILE, MAKE_FILELINE);
					}
				}

				return retVal;
			}
		}
	}
	spdlog::error("Could not find Asset for trade id: {}, {}", tradeId, MAKE_FILELINE);
	return {};
}

void saveAssetForTradeId(const std::string &asset, int tradeId) {
	try {
		std::ifstream ifs(OPEN_TRADES_FILE);
		nlohmann::json json;
		std::map<int, std::string> openTrades;

		if (ifs.is_open()) {
			json = nlohmann::json::parse(ifs);
			ifs.close();
		}

		auto it = json.find("openTrades");

		if (it != json.end()) {
			openTrades = it->get<std::map<int, std::string> >();
			openTrades.insert_or_assign(tradeId, asset);
		} else {
			openTrades.insert_or_assign(tradeId, asset);
		}
		json["openTrades"] = openTrades;

		if (std::ofstream ofs(OPEN_TRADES_FILE); ofs.is_open()) {
			ofs << json.dump(4);
			ofs.close();
		} else {
			spdlog::error("Couldn't save json file, path: {}, {}", OPEN_TRADES_FILE, MAKE_FILELINE);
		}
	} catch (std::exception &e) {
		spdlog::error("{}: {}", MAKE_FILELINE, e.what());
	}
}

DLLFUNC_C int BrokerLogin(char *User, char *Pwd, char *Type, char *Account) {
	if (!User) {
		stopSymbolsUpdater();
		restClient.reset();
		streamManager.reset();
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
	if (!streamManager) {
		spdlog::critical("{}: {}", MAKE_FILELINE, "Bybit WS stream manager instance not initialized.");
		return 0;
	}

	if (pPip != nullptr) {
		try {
			const auto symbols = restClient->getInstrumentsInfo(Category::linear);

			for (const auto &symbol: symbols) {
				if (symbol.m_symbol == Asset) {
					if (pLotAmount) {
						*pLotAmount = symbol.m_lotSizeFilter.m_qtyStep;
#ifdef _WIN64
						lotAmounts.insert_or_assign(Asset, symbol.m_lotSizeFilter.m_qtyStep);
#endif
					}

					*pPip = symbol.m_priceFilter.m_tickSize;

					if (pPipCost && *pPip != 0.0 && *pLotAmount != 0.0) {
						*pPipCost = *pPip * *pLotAmount;
					}
				}
			}
		} catch (std::exception &e) {
			spdlog::error("{}: {}\n", MAKE_FILELINE, e.what());
			BrokerError("Cannot acquire asset info from server.");
		}
	}

	try {
		/// Check if the Book Ticker Stream is subscribed for the Asset
		streamManager->subscribeTickerStream(Asset);

		if (const auto instrumentInfo = streamManager->readEventTicker(Asset)) {
			const auto &info = *instrumentInfo;

			if (info.m_ask1Price == 0.0 || info.m_bid1Price == 0.0) {
				return 0;
			}

			if (pPrice) {
				*pPrice = info.m_lastPrice;
			}
			if (pSpread) {
				*pSpread = info.m_ask1Price - info.m_bid1Price;
			}
			if (pVolume) {
				/// Bybit has no relevant volume info in instrumentInfo stream
			}

			return 1;
		}
		const auto msg = "Could not read Book Ticker Stream for Asset: " + std::string(Asset) + ", reading timeout";
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

		if (pdBalance) {
			double totalBalance = 0.0;
			for (const auto &el: accountBalances.m_balances) {
				for (const auto &elCoin: el.m_coins) {
					if (elCoin.m_coin == accountCurrency) {
						totalBalance += elCoin.m_walletBalance;
					}
				}
			}
			*pdBalance = totalBalance;
			return 1;
		}
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

#ifdef ENABLE_SERVER_TIME
	// Off by default to save time, response takes roughly 0.35 s
	std::int64_t timeInMs = restClient->getServerTime();

	if (pTimeGMT) {
		*pTimeGMT = convertTime(timeInMs);
	}
#endif

	/// Bybit never closes
	return ExchangeStatus::Open;
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
			ticks->fOpen = static_cast<float>(candles[i].m_open);
			ticks->fHigh = static_cast<float>(candles[i].m_high);
			ticks->fLow = static_cast<float>(candles[i].m_low);
			ticks->fClose = static_cast<float>(candles[i].m_close);
			ticks->fVol = static_cast<float>(candles[i].m_volume);

			/// Zorro uses reversed order in time series so that's why...
			ticks->time = convertTime(candles[i].m_startTime + msInInterval);
		}

		return maxCandles;
	} catch (std::exception &e) {
		spdlog::error("{}: {}", MAKE_FILELINE, e.what());
		BrokerError("Cannot acquire historical data from server.");
	}

	return 0;
}

DLLFUNC_C int BrokerBuy2(char *Asset, int Amount, double dStopDist, double Limit, double *pPrice, int *pFill) {
	if (!restClient) {
		spdlog::critical("{}: {}", MAKE_FILELINE, "Bybit Rest Client instance not initialized.");
		return 0;
	}

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
		order.m_symbol = Asset;

		if (Amount > 0) {
			order.m_side = Side::Buy;
		} else {
			order.m_side = Side::Sell;
		}

		if (orderType == 1) {
			order.m_timeInForce = TimeInForce::IOC;
		} else if (orderType == 2) {
			order.m_timeInForce = TimeInForce::GTC;
		} else {
			order.m_timeInForce = TimeInForce::FOK;
		}

		if (Limit > 0.) {
			order.m_price = Limit;
			order.m_orderType = OrderType::Limit;
		} else {
			order.m_orderType = OrderType::Market;
		}

		order.m_qty = lotAmount * std::abs(Amount);

		readBybitLastOrderId();
		order.m_orderLinkId = std::to_string(lastOrderId++);
		writeBybitLastOrderId();

		auto positionResponse = restClient->getPositionInfo(Category::linear, Asset);

		if (!positionResponse.empty()) {
			if (positionResponse[0].m_positionIdx) {
				if (order.m_side == Side::Buy) {
					order.m_positionIdx = 1;
				} else if (order.m_side == Side::Sell) {
					order.m_positionIdx = 2;
				}
			} else {
				order.m_positionIdx = 0;
			}
		}

		auto orderId = restClient->placeOrder(order);
		OrderResponse orderResponse{};

		int attemptNo = 0;

		while (orderResponse.m_orderStatus != OrderStatus::Active &&
		       orderResponse.m_orderStatus != OrderStatus::Filled) {
			const auto activeOrder = restClient->getOpenOrder(Category::linear, Asset, orderId.m_orderId,
			                                                  orderId.m_orderLinkId);

			if (activeOrder) {
				orderResponse.m_orderStatus = activeOrder->m_orderStatus;
				orderResponse.m_avgPrice = activeOrder->m_avgPrice;
				orderResponse.m_cumExecQty = activeOrder->m_cumExecQty;
				orderResponse.m_orderLinkId = activeOrder->m_orderLinkId;
			}

			attemptNo++;

			if (int maxAttempts = 10; attemptNo == maxAttempts) {
				spdlog::error("Send order failed due timeout: attemptNo == maxAttempts");
				spdlog::error(
					"Cannot send order to server, reason: market order was not filled or a trigger order was not activated, order response: {}",
					orderResponse.toJson().dump());
				BrokerError("Cannot send order to server.");
				return 0;
			}

			std::this_thread::sleep_for(500ms);
		}

		if (pPrice) {
			*pPrice = orderResponse.m_avgPrice;
		}

		if (pFill) {
			*pFill = std::round(orderResponse.m_cumExecQty / lotAmount);
		}

		spdlog::info("Order placed for asset: " + std::string(Asset) + ", filled size: " +
		             std::to_string(orderResponse.m_cumExecQty / lotAmount) + ", average price: " +
		             std::to_string(orderResponse.m_avgPrice) + ", clientId: " + orderResponse.m_orderLinkId);

		saveAssetForTradeId(Asset, stoi(orderResponse.m_orderLinkId));
		return stoi(orderResponse.m_orderLinkId);
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

	try {
		auto asset = findAssetForTradeId(nTradeId);

		if (asset.empty()) {
			return 0;
		}

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
		order.m_symbol = asset;

		if (nAmount > 0) {
			order.m_side = Side::Sell;
		} else {
			order.m_side = Side::Buy;
		}

		if (Limit > 0.) {
			order.m_price = Limit;
			order.m_orderType = OrderType::Limit;
		} else {
			order.m_orderType = OrderType::Market;
		}

		order.m_qty = lotAmount * std::abs(nAmount);

		readBybitLastOrderId();
		order.m_orderLinkId = std::to_string(lastOrderId++);
		writeBybitLastOrderId();

		order.m_timeInForce = TimeInForce::GTC;

		auto positionResponse = restClient->getPositionInfo(Category::linear, asset);

		if (!positionResponse.empty()) {
			if (positionResponse[0].m_positionIdx) {
				if (order.m_side == Side::Buy) {
					order.m_positionIdx = 2;
				} else if (order.m_side == Side::Sell) {
					order.m_positionIdx = 1;
				}
			} else {
				order.m_positionIdx = 0;
			}
		}

		auto orderId = restClient->placeOrder(order);
		OrderResponse orderResponse{};

		int attemptNo = 0;

		while (orderResponse.m_orderStatus != OrderStatus::Active &&
		       orderResponse.m_orderStatus != OrderStatus::Filled) {
			const auto activeOrder = restClient->getOpenOrder(Category::linear, asset, orderId.m_orderId,
			                                                  orderId.m_orderLinkId);

			if (activeOrder) {
				orderResponse.m_orderStatus = activeOrder->m_orderStatus;
				orderResponse.m_avgPrice = activeOrder->m_avgPrice;
				orderResponse.m_cumExecQty = activeOrder->m_cumExecQty;
				orderResponse.m_orderLinkId = activeOrder->m_orderLinkId;
			}

			attemptNo++;

			if (int maxAttempts = 10; attemptNo == maxAttempts) {
				spdlog::error("Send order failed due timeout: attemptNo == maxAttempts");
				spdlog::error(
					"Cannot send order to server, reason: market order was not filled or a trigger order was not activated, order response: {}",
					orderResponse.toJson().dump());
				BrokerError("Cannot send order to server.");
				return 0;
			}

			std::this_thread::sleep_for(500ms);
		}

		if (pFill) {
			*pFill = std::round(orderResponse.m_cumExecQty / lotAmount);
		}

		spdlog::info("Closed order placed for asset: " + std::string(asset) + ", filled size: " +
		             std::to_string(orderResponse.m_cumExecQty / lotAmount) + ", average price: " +
		             std::to_string(orderResponse.m_avgPrice) + ", clientId: " + orderResponse.m_orderLinkId);

		/// Just erase the tradeId
		findAssetForTradeId(nTradeId, true);

		return stoi(orderResponse.m_orderLinkId);
	} catch (std::exception &e) {
		spdlog::error("{}: {}", MAKE_FILELINE, e.what());
		BrokerError("Cannot close trade.");
	}


	return 0;
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
						if (position.m_side == Side::Sell) {
							totalPositionAmt -= position.m_size;
						} else {
							totalPositionAmt += position.m_size;
						}
					}

					/// Return real position size instead of lot amount
					/// totalPositionAmt = totalPositionAmt / lotAmount;

					return totalPositionAmt;
				} catch (std::exception &e) {
					spdlog::error("{}: {}", MAKE_FILELINE, e.what());
					BrokerError((std::string("Cannot get position of " + std::string(symbol)).c_str()));
				}
			}
			break;
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
					             id.m_orderLinkId);
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

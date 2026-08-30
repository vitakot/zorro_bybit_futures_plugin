# Changelog
All notable changes to this project will be documented in this file. This project adheres to [Semantic Versioning](http://semver.org/).

## 2.0.0 (2026-08-29)

### Changed

- `recvWindow` of signed requests lowered from the 60 s maximum to the exchange default of 5 s. With the clock
  synchronization in place the wide window only meant that a badly delayed order could still be executed.
- `GET_POSITION` now returns the net open amount in lots, as the broker API requires ("net open amount as in
  BrokerBuy2"). It used to return the raw contract quantity. **Scripts that relied on the old unit must be adapted.**
- `BrokerBuy2` no longer answers a failed send with a plain "rejected". A failure whose outcome is unknown (the
  transport, the HTTP 5xx family, and the API codes 10016 / 170007) is reconciled: the order is queried back by its client order id,
  an order still resting on the book is cancelled - which the broker API requires after answering -2 - and whatever
  filled is reported as the trade.
- `BrokerAsset` reports the ask price instead of the last traded price, which is what Zorro expects and what the
  returned spread is relative to.
- `BrokerBuy2` returns -2 when the exchange did not confirm the order (network, timeout, unresolved state) instead
  of 0. A transport failure does not mean the order was rejected - it may well be live - and 0 would invite a
  duplicate order.

### Added

- `GET_COMPLIANCE` returns "no hedging" for One-way accounts, so Zorro nets opposite entries instead of believing it
  holds two independent positions.
- `BrokerTime` detects a lost connection: it reports the exchange as unreachable when neither recent traffic nor a
  probe request succeeds, which is what lets Zorro stop trading and reconnect.
- `BrokerTrade`, so that Zorro can follow the fill state of orders resting on the book.

### Fixed

- **A partially filled entry left the trade record wrong.** The record kept the REQUESTED size, so an order for 10
  lots that filled 3 and was then fully closed still counted 7 lots as open and `BrokerTrade` kept reporting an open
  position that no longer existed. Closed lots are now counted cumulatively against the real entry fill.
- The store was written by opening the target file directly, which truncates it first - a crash or a full disk in
  the middle of the write destroyed the trade to symbol mapping. It is written to a temporary file and renamed over
  the target now, and a failed write is reported to Zorro instead of only being logged.
- Blocking socket operations had no bound at all, so a black holed connection could stall the Zorro thread until the
  operating system gave up, which is minutes. Every read and write of a request is now bounded by the `SET_WAIT`
  time. The connect itself still falls back to the Windows connect timeout.
- `setCredentials` replaced the HTTP session without synchronization while the background instrument updater was
  using it - a data race, and the `reset()` before the assignment even left a window with a null session.
- The position mode is remembered from the position records so that `GET_COMPLIANCE` can report it; Bybit has no
  account wide query for it and One-way, the venue default, is assumed until an order reveals otherwise.
- **The plugin did not compile.** Every member access still used the old `m_` prefixed field names that
  bybit-cpp-api dropped.
- Subscribing an illiquid asset failed. Zorro subscribes an asset by calling `BrokerAsset` with `pPrice == NULL`,
  and an asset that answers 0 to that call triggers Error 053 and gets its trading disabled. The subscription call
  no longer depends on a quote having arrived. In addition the quote is seeded from a REST snapshot on subscribe and
  a REST snapshot is used as a fallback whenever the stream stays silent.
- Read timeout of the stream manager was 0.3x the configured value (a `timeout / 0.01` counter combined with a 3 ms
  sleep), so the effective wait was 1.5 s instead of 5 s. Replaced by a real deadline.
- A cached quote is no longer served forever after the WebSocket session dies - quotes older than `maxTickAge`
  (60 s) are refreshed over REST instead.
- The order state loop waited for the v3-era status `Active`, which Bybit v5 never reports for a resting order. A
  GTC limit order therefore polled for 5 s and was then reported as a failure while it was live on the exchange.
  Order states are now classified explicitly and a resting order is reported as a pending trade.
- A partially filled IOC order (`PartiallyFilledCanceled`) was reported as a failure although the fill is a real
  position. Partial fills are now reported with their fill amount; an order still unsettled after the poll budget
  returns -2 (state unknown) instead of a false rejection.
- `BrokerSell2` dropped the trade id to symbol mapping before the closing order was confirmed, so a failed close
  left the trade impossible to close ever again. It then tried to erase the same record a second time, logging a
  bogus "Could not find Asset for trade id" error on every close.
- `BrokerSell2` returned the id of the closing order. Zorro expects the original trade id unless the broker really
  assigned a new one to the remainder.
- One-way closes are sent with `reduceOnly`, so an oversized close cannot flip the position.
- `BrokerAccount` reported neither the trade value nor the bound margin, and returned 0 when only the balance
  pointer was missing.
- `BrokerAsset` returned success for an unknown symbol and dereferenced `pLotAmount` without a null check.
- Signed requests used the local clock without any correction, so a clock drift beyond recv_window made every
  request fail. The offset against the exchange clock is now measured and applied.
- Ticker cache entries were keyed by the symbol from the message body, which a delta message does not carry, while
  lookups used the symbol from the topic.
- Beast's default 8 MB body limit rejected the larger public data archives.
- The instrument list was re-downloaded every 60 seconds, now every 15 minutes, and hot paths no longer copy the
  whole several hundred entry list just to read one value.
- `getOpenOrder()` parsed the response body twice.
- The Visual Studio project pointed at `bybit_cpp_api` / `vk_cpp_common`, which no longer exist, listed template
  headers that were never in the repository, did not link zlib although it compiles the zlib using REST client, and
  carried the Binance project's GUID and root namespace.
- **A closing order could leave Zorro holding a trade that no longer exists.** A GTC limit close rested on the book,
  nothing tracked it afterwards, and a fill that arrived later was never booked - `BrokerTrade` kept reporting the
  entry fill as open. Closes are sent IOC now, so they always reach a terminal state before the call returns, and a
  close whose outcome could not be established is remembered and settled by `BrokerTrade`.
- **Reconciliation of an unknown outcome could report a live order as rejected.** Cancelling is asynchronous, so the
  single query after the cancel could still answer "working" with nothing filled, which was taken for "did not
  fill". The order is now polled until the exchange settles it, and an order that does not settle is reported as
  unknown (-2), never as a rejection.
- **Any error during reconciliation was read as "the order does not exist".** An authentication failure, a rate
  limit or a malformed answer says nothing about the order. Only the venue's own "order does not exist" (Binance
  -2013, Bybit 110001/170213) settles it now, through a dedicated `OrderNotFound` exception; everything else keeps
  the outcome unknown.
- **Bybit `BrokerTrade` ignored the closed lots for a terminal entry order.** An IOC entry that filled partially and
  was then fully closed still reported the original fill as open, because the terminal branch returned early and
  skipped the `entry fill - closed` calculation.
- Bybit no longer answers -2 for an order whose state the poll budget could not resolve without first trying to
  cancel it, which the broker API requires.
- Binance HTTP 408 is treated as an unknown outcome as well - it is a timeout waiting for the backend, so the
  request may have reached it.
- `stonky_common` is linked `PUBLIC`, so its include directories reach every consumer. Without it a clean CMake
  configure with `ENABLE_TESTS=ON` failed to build the test executables.

- **A closing order is no longer retried while an earlier one is unresolved.** `BrokerSell2` now settles every
  pending close of the trade before sending a new one, and refuses to send anything while one stays unknown. A
  retry would have been a second reduce-only order against the aggregated exchange position, whose fill would be
  booked against this trade although it may have closed lots belonging to another one.
- The reconciled fill no longer loses its price: the resolver returns the average fill price along with the lots,
  so `pPrice` and `pClose` are filled in the timeout paths as well.
- **`BrokerBuy2` could still answer -2 without ever attempting a cancel**, in both of its unresolved paths. The fix
  claimed for this in the previous round was lost before it was committed; both paths go through the reconciliation
  now.
- **An empty answer from the realtime order endpoint was taken as proof.** Bybit only serves working orders there
  and warns the endpoint can lag, so "not listed and no fills yet" was read as "never filled" and reported as a
  plain rejection while the order could be live. It is proof only when a fill exists, when the order reaches a
  terminal state, or when a cancel probe is answered with "order does not exist"; otherwise the outcome stays
  unknown.

- **An accepted cancel request was treated as a terminal state.** Bybit only acknowledges that it took the request;
  the cancellation is processed asynchronously and the order can still fill meanwhile. After the poll budget the
  resolver returned the current (often empty) execution list as the final outcome, so a live order could be reported
  as "did not fill" - and a pending close was dropped from the record on the same basis. Without an observed
  terminal state the outcome now stays unknown and the caller answers -2.
- The order history is consulted when the realtime endpoint does not list the order. That endpoint only serves
  working orders, so it is where a finished order carries its terminal state; relying on the execution list alone
  could not tell "finished" from "not visible yet".
- `getExecutions()` follows `nextPageCursor` and asks for pages of 100. It read only the first page before, so an
  order that filled in more than 50 pieces would have had its fill understated during reconciliation.

### Notes

- The plugin API level reported by `BrokerOpen` stays at 2, that is the Zorro broker API generation the plugin
  implements, not the plugin's own version.

## 1.0.0 (2025-10-13)

- Initial release
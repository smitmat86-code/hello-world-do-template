import { DurableObject } from "cloudflare:workers";

// ------------------------
// GLOBALS
// ------------------------

let MASTER_WATCHLIST: string[] = [];
let MASTER_WATCHLIST_DAY: string | null = null;

// ------------------------
// MAIN WORKER EXPORT
// ------------------------

const worker = {
  async fetch(request: Request, env: any, ctx: any): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    if (path === "/status") {
      return json({ ok: true, message: "trading bot worker is running" });
    }

    if (path === "/debug/run-once") {
      const result = await runBotOnce(env, { force: true });
      return json(result);
    }

    return json({ ok: false, error: "Not found" }, 404);
  },

  async scheduled(event: any, env: any, ctx: any): Promise<void> {
    ctx.waitUntil(runBotOnce(env, { force: false }));
  }
};

export default worker;

// ------------------------
// DURABLE OBJECT: TradingBotState
//   - tracks per-day equity, P&L, daily max-loss, consecutive losses
// ------------------------

export class TradingBotState extends DurableObject {
  private data: {
    date: string | null;
    startEquity: number | null;
    hitDailyMaxLoss: boolean;
    consecutiveLosses: number;
    dailyMaxLossPct: number | null;
  };
  private loaded: boolean;

  constructor(ctx: any, env: any) {
    super(ctx, env);
    this.data = {
      date: null,
      startEquity: null,
      hitDailyMaxLoss: false,
      consecutiveLosses: 0,
      dailyMaxLossPct: null
    };
    this.loaded = false;
  }

  private async loadState() {
    if (this.loaded) return;
    const stored: any = await this.ctx.storage.get("botState");
    if (stored && typeof stored === "object") {
      this.data = {
        date: stored.date ?? null,
        startEquity:
          typeof stored.startEquity === "number" ? stored.startEquity : null,
        hitDailyMaxLoss: !!stored.hitDailyMaxLoss,
        consecutiveLosses:
          typeof stored.consecutiveLosses === "number"
            ? stored.consecutiveLosses
            : 0,
        dailyMaxLossPct:
          typeof stored.dailyMaxLossPct === "number"
            ? stored.dailyMaxLossPct
            : null
      };
    }
    this.loaded = true;
  }

  private async saveState() {
    await this.ctx.storage.put("botState", this.data);
  }

  async fetch(request: Request): Promise<Response> {
    if (request.method !== "POST") {
      return new Response("Method Not Allowed", { status: 405 });
    }

    let body: any;
    try {
      body = await request.json();
    } catch {
      return new Response("Bad Request", { status: 400 });
    }

    const action = body.action;

    if (action === "getOrUpdate") {
      const { date, equity, dailyMaxLossPct } = body;
      const result = await this.handleGetOrUpdate(
        String(date),
        equity,
        dailyMaxLossPct
      );
      return new Response(JSON.stringify(result), {
        status: 200,
        headers: { "Content-Type": "application/json" }
      });
    }

    if (action === "registerTradeResult") {
      const { date, pnl } = body;
      const result = await this.handleRegisterTradeResult(
        String(date),
        pnl
      );
      return new Response(JSON.stringify(result), {
        status: 200,
        headers: { "Content-Type": "application/json" }
      });
    }

    return new Response("Unknown action", { status: 400 });
  }

  private async handleGetOrUpdate(
    date: string,
    equityRaw: number | string,
    dailyMaxLossPctRaw: number | string
  ) {
    await this.loadState();

    const eq =
      typeof equityRaw === "number"
        ? equityRaw
        : parseFloat(String(equityRaw) || "0");
    const maxLossPct =
      typeof dailyMaxLossPctRaw === "number"
        ? dailyMaxLossPctRaw
        : parseFloat(String(dailyMaxLossPctRaw) || "0.03");

    // New trading day or no baseline yet → reset
    if (this.data.date !== date || this.data.startEquity == null) {
      this.data.date = date;
      this.data.startEquity = eq;
      this.data.hitDailyMaxLoss = false;
      this.data.consecutiveLosses = 0;
      this.data.dailyMaxLossPct = maxLossPct;
    } else {
      this.data.dailyMaxLossPct = maxLossPct;
    }

    let currentDayPL: number | null = null;
    if (
      typeof eq === "number" &&
      typeof this.data.startEquity === "number"
    ) {
      currentDayPL = eq - this.data.startEquity;
      const maxLoss =
        -Math.abs(
          this.data.startEquity *
            (this.data.dailyMaxLossPct ?? maxLossPct)
        );
      if (currentDayPL <= maxLoss) {
        this.data.hitDailyMaxLoss = true;
      }
    }

    await this.saveState();

    return {
      date: this.data.date,
      startEquity: this.data.startEquity,
      currentDayPL,
      hitDailyMaxLoss: this.data.hitDailyMaxLoss,
      consecutiveLosses: this.data.consecutiveLosses,
      dailyMaxLossPct: this.data.dailyMaxLossPct
    };
  }

  private async handleRegisterTradeResult(
    date: string,
    pnlRaw: number | string
  ) {
    await this.loadState();

    if (this.data.date !== date) {
      // New day → reset counters
      this.data.date = date;
      this.data.consecutiveLosses = 0;
    }

    const pnl =
      typeof pnlRaw === "number"
        ? pnlRaw
        : parseFloat(String(pnlRaw) || "0");

    if (pnl < 0) {
      this.data.consecutiveLosses += 1;
    } else if (pnl > 0) {
      this.data.consecutiveLosses = 0;
    }

    await this.saveState();

    return {
      date: this.data.date,
      consecutiveLosses: this.data.consecutiveLosses
    };
  }
}

// ------------------------
// MAIN BOT RUN
// ------------------------

async function runBotOnce(env: any, options: { force: boolean }) {
  const force = options?.force === true;

  const now = new Date();
  const { nyMinutes, nyDayKey } = getNewYorkTimeInfo(now);

  log(
    `Run start. NY day=${nyDayKey}, minutes=${nyMinutes}, force=${force}`
  );

  // Trading window: 09:24 → 11:00 ET
  const ENTRY_WINDOW_START = 9 * 60 + 24;
  const ENTRY_WINDOW_END = 11 * 60;

  if (!force) {
    if (nyMinutes < ENTRY_WINDOW_START) {
      log("Too early (before 09:24 ET). Skipping scan.");
      return { ok: true, reason: "too-early" };
    }
    if (nyMinutes > ENTRY_WINDOW_END) {
      log("Too late (after 11:00 ET). Skipping scan.");
      return { ok: true, reason: "too-late" };
    }
  } else {
    log("Force=true → bypassing time-of-day entry restrictions.");
  }

  // 1) Build / reuse H2 master watchlist from Massive
  const watchlist = await getOrBuildMasterWatchlist(env, nyDayKey);

  if (!watchlist || watchlist.length === 0) {
    log("MASTER_WATCHLIST is empty. Nothing to scan.");
    return { ok: false, reason: "empty-watchlist" };
  }

  log(`Using MASTER_WATCHLIST of size=${watchlist.length}.`);

  // 2) Fetch account (for equity + risk sizing)
  const account = await fetchAlpacaAccount(env);
  if (!account) {
    log("Failed to fetch Alpaca account; aborting run.");
    return { ok: false, reason: "account-fetch-failed" };
  }

  const equity = parseFloat(String(account.equity ?? "0"));
  log(
    `Account equity=${account.equity}, buying_power=${account.buying_power}`
  );

  // 3) Talk to Durable Object for daily risk state
  const riskState = await getRiskState(env, nyDayKey, equity);
  log(
    `Risk state for ${riskState.date}: startEquity=${riskState.startEquity}, dayPL=${riskState.currentDayPL}, hitDailyMaxLoss=${riskState.hitDailyMaxLoss}, consecutiveLosses=${riskState.consecutiveLosses}, maxLossPct=${riskState.dailyMaxLossPct}`
  );

  if (riskState.hitDailyMaxLoss && !force) {
    log(
      `🚫 Daily max loss reached (dayPL=${riskState.currentDayPL}). Skipping new entries.`
    );
    return { ok: true, reason: "daily-max-loss-hit", riskState };
  }

  // 4) Fetch positions (for one-bullet rule)
  const positions = await fetchAlpacaPositions(env);
  const positionsBySymbol: Record<string, any> = {};
  for (const p of positions) {
    positionsBySymbol[p.symbol] = p;
  }

  log(`Currently open positions=${positions.length}`);

  // 5) H3 entry logic: MACD + Pullback + ABCD
  const scanResult = await scanAndTrade_H3(env, {
    watchlist,
    positionsBySymbol,
    account
  });

  return {
    ok: true,
    riskState,
    ...scanResult
  };
}

// ------------------------
// H2: Massive watchlist builder
// ------------------------

async function getOrBuildMasterWatchlist(env: any, nyDayKey: string) {
  if (MASTER_WATCHLIST_DAY === nyDayKey && MASTER_WATCHLIST.length > 0) {
    log(
      `✅ Reusing MASTER_WATCHLIST from ${MASTER_WATCHLIST_DAY} (size=${MASTER_WATCHLIST.length}).`
    );
    return MASTER_WATCHLIST;
  }

  log(
    `🛠 MASTER_WATCHLIST missing or stale (dayKey=${MASTER_WATCHLIST_DAY}), building for ${nyDayKey}…`
  );

  const massiveApiKey = env.MASSIVE_API_KEY;
  const massiveBase = env.MASSIVE_BASE_URL || "https://api.massive.com";

  if (!massiveApiKey) {
    log("Massive API key missing; cannot build watchlist.");
    MASTER_WATCHLIST = [];
    MASTER_WATCHLIST_DAY = nyDayKey;
    return MASTER_WATCHLIST;
  }

  const url =
    massiveBase +
    `/v2/snapshot/locale/us/markets/stocks/tickers?apiKey=${encodeURIComponent(
      massiveApiKey
    )}`;

  log(`⏱ Calling Massive snapshot: ${url}`);

  let data: any;
  try {
    const res = await fetch(url);
    if (!res.ok) {
      log(
        `Massive snapshot HTTP ${res.status} – ${await res
          .text()
          .catch(() => "")}`
      );
      MASTER_WATCHLIST = [];
      MASTER_WATCHLIST_DAY = nyDayKey;
      return MASTER_WATCHLIST;
    }
    data = await res.json();
  } catch (err) {
    log(`Massive snapshot error: ${err}`);
    MASTER_WATCHLIST = [];
    MASTER_WATCHLIST_DAY = nyDayKey;
    return MASTER_WATCHLIST;
  }

  const tickers: any[] = data?.tickers || [];
  log(
    `Massive snapshot: received ${tickers.length} tickers, normalized… (H2 filters)`
  );

  const PRICE_MIN = parseFloat(env.PRICE_MIN ?? "5");
  const PRICE_MAX = parseFloat(env.PRICE_MAX ?? "200");
  const PCT_CHANGE_MIN = parseFloat(env.PCT_CHANGE_MIN ?? "2");
  const REL_VOL_MIN = parseFloat(env.REL_VOL_MIN ?? "1.5");
  const VOL_MIN = parseFloat(env.VOL_MIN ?? "500000");
  const MAX_SCREEN = parseInt(env.MAX_SCREEN ?? "100", 10);

  let priceOk = 0;
  let pctOk = 0;
  let relVolOk = 0;
  let volOk = 0;
  let allOk = 0;

  const passed: string[] = [];

  for (const t of tickers) {
    const symbol = t.ticker;
    const day = t.day || {};
    const min = t.min || {};

    const price = typeof day.c === "number" ? day.c : null;
    const pctChange =
      typeof t.todaysChangePerc === "number" ? t.todaysChangePerc : null;
    const vol = typeof day.v === "number" ? day.v : null;

    let relVol: number | null = null;
    if (typeof min.av === "number" && min.av > 0 && typeof day.v === "number") {
      relVol = day.v / min.av;
    }

    let ok = true;

    if (price == null || price < PRICE_MIN || price > PRICE_MAX) {
      ok = false;
    } else {
      priceOk++;
    }

    if (pctChange == null || Math.abs(pctChange) < PCT_CHANGE_MIN) {
      ok = false;
    } else {
      pctOk++;
    }

    if (relVol == null || relVol < REL_VOL_MIN) {
      ok = false;
    } else {
      relVolOk++;
    }

    if (vol == null || vol < VOL_MIN) {
      ok = false;
    } else {
      volOk++;
    }

    if (ok) {
      allOk++;
      passed.push(symbol);
      if (passed.length >= MAX_SCREEN) break;
    }
  }

  log(
    `H2 filter stats: total=${tickers.length}, price_ok=${priceOk}, pct_ok=${pctOk}, relVol_ok=${relVolOk}, vol_ok=${volOk}, all_ok=${allOk}`
  );

  if (allOk === 0) {
    log("⚠ No symbols passed numeric filters; MASTER_WATCHLIST stays empty.");
  } else {
    log(`✅ MASTER_WATCHLIST (H2) built: ${passed.length} symbols.`);
  }

  MASTER_WATCHLIST = passed;
  MASTER_WATCHLIST_DAY = nyDayKey;
  return MASTER_WATCHLIST;
}

// ------------------------
// H3: MACD + Pullback + ABCD entries
// ------------------------

async function scanAndTrade_H3(
  env: any,
  {
    watchlist,
    positionsBySymbol,
    account
  }: { watchlist: string[]; positionsBySymbol: Record<string, any>; account: any }
) {
  const symbols = watchlist;
  const DRY_RUN = (env.DRY_RUN ?? "true").toLowerCase() === "true";
  const riskPct = parseFloat(env.RISK_PCT_PER_TRADE ?? "0.01"); // 1% default

  log(`Scanning ${symbols.length} symbols for H3 entries (PULLBACK or ABCD)…`);

  let scanned = 0;
  let signals = 0;
  const actions: any[] = [];

  for (const symbol of symbols) {
    scanned++;

    // One-bullet: skip if we already hold it
    if (positionsBySymbol[symbol]) {
      log(`🔁 ${symbol}: position already open, skipping (one bullet rule).`);
      continue;
    }

    // Get last 60 1-min bars for MACD + ABCD structure
    const bars: any[] | null = await fetch1MinBars(env, symbol, 60);
    if (!bars || bars.length < 30) {
      log(
        `⚠ ${symbol}: not enough 1-min bars (${bars?.length ?? 0}), skipping.`
      );
      continue;
    }

    // bars = oldest → newest
    const current = bars[bars.length - 1]; // newest (still forming)
    const last = bars[bars.length - 2]; // last completed
    const prev = bars[bars.length - 3]; // candle before that

    // --- MACD check (red light / green light) ---
    const closes = bars.map((b) => b.c);
    const macd = calculateMACD(closes, 12, 26, 9);
    if (!macd) {
      log(`⚠ ${symbol}: could not compute MACD, skipping.`);
      continue;
    }

    const isMacdPositive = macd.line > macd.signal;
    if (!isMacdPositive) {
      log(
        `⛔ ${symbol}: MACD not positive (line=${macd.line.toFixed(
          4
        )}, signal=${macd.signal.toFixed(4)}), skipping.`
      );
      continue;
    }

    // --- Pattern A: Micro Pullback / Bull Flag ---
    const prevGreen = isGreen(prev);
    const lastRed = isRed(last);
    const isPullback = prevGreen && lastRed;
    const entryTriggerPullback = last.h;

    // --- Pattern B: ABCD Pattern ---
    let isABCDSetup = false;
    let entryTriggerABCD: number | null = null;
    let pointCPrice: number | null = null;

    if (bars.length >= 30) {
      const lookback = 30;
      const startIdx = Math.max(0, bars.length - lookback);
      let bIdx = startIdx;
      let bHigh = bars[startIdx].h;

      for (let i = startIdx + 1; i < bars.length; i++) {
        if (bars[i].h > bHigh) {
          bHigh = bars[i].h;
          bIdx = i;
        }
      }

      const pointBPrice = bHigh;

      if (bIdx < bars.length - 1) {
        let lowest = bars[bIdx + 1].l;
        for (let i = bIdx + 2; i < bars.length; i++) {
          if (bars[i].l < lowest) lowest = bars[i].l;
        }
        pointCPrice = lowest;

        const sessionOpen = getSessionOpenPrice(bars);
        const isHigherLow = pointCPrice > sessionOpen;

        const isCurlingUp =
          typeof current.c === "number" &&
          current.c > pointCPrice &&
          current.c < pointBPrice;

        isABCDSetup = isHigherLow && isCurlingUp;
        entryTriggerABCD = pointBPrice;

        log(
          `📐 ${symbol} ABCD check: B=${pointBPrice.toFixed(
            2
          )}, C=${pointCPrice.toFixed(
            2
          )}, sessionOpen=${sessionOpen.toFixed(
            2
          )}, higherLow=${isHigherLow}, curlingUp=${isCurlingUp}`
        );
      } else {
        log(
          `ℹ️ ${symbol}: ABCD – B too close to latest bar, skipping C search.`
        );
      }
    }

    // --- Choose pattern & trigger ---
    let triggerPrice = 0;
    let pattern: "PULLBACK" | "ABCD" | null = null;

    if (isPullback) {
      triggerPrice = entryTriggerPullback;
      pattern = "PULLBACK";
    } else if (isABCDSetup && entryTriggerABCD != null) {
      triggerPrice = entryTriggerABCD;
      pattern = "ABCD";
    }

    if (!(pattern && triggerPrice > 0)) {
      log(
        `ℹ️ ${symbol}: no valid PULLBACK or ABCD setup (pullback=${isPullback}, abcd=${isABCDSetup}).`
      );
      continue;
    }

    const isHighVolume = current.v > last.v;
    const priceNow = current.c;

    if (!(priceNow >= triggerPrice && isHighVolume)) {
      log(
        `ℹ️ ${symbol} [${pattern}]: breakout not confirmed (priceNow=${priceNow.toFixed(
          2
        )}, trigger=${triggerPrice.toFixed(
          2
        )}, volNow=${current.v}, volPrev=${last.v}).`
      );
      continue;
    }

    // --- Entry signal fired ---
    signals++;

    // --- Sizing (equity-based risk per trade) ---
    const equity = parseFloat(String(account.equity ?? "0"));
    const riskDollars = equity * riskPct;

    let riskPerShare: number;
    if (pattern === "PULLBACK") {
      riskPerShare = triggerPrice - last.l;
    } else if (pattern === "ABCD" && pointCPrice != null) {
      riskPerShare = triggerPrice - pointCPrice;
    } else {
      riskPerShare = priceNow * 0.01;
    }

    if (!isFinite(riskPerShare) || riskPerShare <= 0) {
      riskPerShare = priceNow * 0.01;
    }

    let shares = Math.floor(riskDollars / riskPerShare);
    if (!isFinite(shares) || shares <= 0) {
      log(
        `⚠ ${symbol} [${pattern}]: sizing non-positive (risk$=${riskDollars.toFixed(
          2
        )}, risk/share=${riskPerShare.toFixed(2)}), skipping order.`
      );
      continue;
    }

    actions.push({
      symbol,
      pattern,
      priceNow,
      triggerPrice,
      shares,
      riskDollars,
      riskPerShare
    });

    if (DRY_RUN) {
      log(
        `🧪 DRY_RUN: would BUY ${shares} ${symbol} [${pattern}] @ ~${priceNow.toFixed(
          2
        )} (trigger=${triggerPrice.toFixed(
          2
        )}, risk≈$${riskDollars.toFixed(
          2
        )}, risk/share≈$${riskPerShare.toFixed(2)}).`
      );
    } else {
      const order = await placeMarketOrder(env, {
        symbol,
        qty: shares,
        side: "buy"

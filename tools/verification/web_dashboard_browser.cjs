"use strict";
const {chromium} = require("playwright");
const fs = require("node:fs");
const path = require("node:path");
const assert = require("node:assert/strict");

const base = process.argv[2] || "http://127.0.0.1:8765";
assert(["127.0.0.1", "localhost"].includes(new URL(base).hostname));
const output = path.resolve(process.argv[3] || ".local_records/web-browser");
assert(output.startsWith(path.resolve(".local_records") + path.sep));
fs.mkdirSync(output, {recursive: true});

(async () => {
  const browser = await chromium.launch({channel: "msedge", headless: true});
  const report = {base, checked_at: new Date().toISOString(), layouts: [], workflows: [], errors: []};
  try {
    const page = await browser.newPage();
    page.on("pageerror", error => report.errors.push(String(error)));
    page.on("console", message => {
      if (message.type() === "error") report.errors.push(message.text());
    });
    async function ready(view) {
      await page.locator(`nav a.active[data-view="${view}"]`).waitFor();
      await page.locator("#content h1").waitFor();
      await page.waitForFunction(() => !document.querySelector("#refresh").disabled);
      assert.match(await page.locator("#source-banner").innerText(), /DEMO/);
    }
    for (const [width, height] of [[1440, 1000], [390, 844], [768, 1024], [320, 740]]) {
      await page.setViewportSize({width, height});
      for (const view of ["overview", "signals", "candidates", "stock", "performance", "health", "settings"]) {
        await page.goto(`${base}/#${view === "stock" ? "stock/DEMO01" : view}`);
        await ready(view);
        await page.locator(".source-details").evaluate(element => {element.open = true;});
        const snapshot = await page.evaluate(() => {
          const canvas = document.querySelector("canvas");
          let coloredPixels = 0;
          if (canvas) {
            const pixels = canvas.getContext("2d").getImageData(0, 0, canvas.width, canvas.height).data;
            for (let i = 0; i < pixels.length; i += 4) {
              if (pixels[i + 3] > 0 && Math.abs(pixels[i] - pixels[i + 1]) > 25) coloredPixels++;
            }
          }
          return {
            title: document.querySelector("h1").textContent,
            overflow: document.documentElement.scrollWidth > innerWidth + 1,
            scrollWidth: document.documentElement.scrollWidth,
            brokenImages: [...document.images].filter(i => !i.complete || i.naturalWidth === 0).map(i => i.src),
            canvas: canvas ? {width: canvas.width, height: canvas.height, coloredPixels} : null,
          };
        });
        report.layouts.push({width, height, view, ...snapshot});
        if (snapshot.overflow) report.errors.push(`${width}/${view}: body overflow ${snapshot.scrollWidth}`);
        if (snapshot.brokenImages.length) report.errors.push(`${width}/${view}: broken images`);
        if (snapshot.canvas && snapshot.canvas.coloredPixels < 100) report.errors.push(`${width}/${view}: blank canvas`);
        if ([1440, 390].includes(width)) {
          await page.screenshot({path: path.join(output, `${width}-${view}.png`), fullPage: true});
        }
      }
    }
    for (const width of [1440, 390]) {
    await page.setViewportSize({width, height: width === 1440 ? 1000 : 844});
    await page.goto(`${base}/#candidates`);
    await ready("candidates");
    await page.locator("#candidate-search").fill("DEMO02");
    assert.equal(await page.locator("#candidate-table tbody tr").count(), 1);
    assert.match(await page.locator("#candidate-count").innerText(), /^1 /);
    await page.locator("#candidate-table a").click();
    await ready("stock");
    assert.equal(await page.locator("h1").innerText(), "演示科技");
    await page.locator('[data-period="30"]').click();
    assert.match(await page.locator('[data-period="30"]').getAttribute("class"), /active/);
    await page.locator("#ma-toggle").uncheck();
    assert.equal(await page.locator("#ma-toggle").isChecked(), false);
    await page.locator("canvas").hover();
    assert.match(await page.locator(".chart-tip").innerText(), /开.*高/s);
    report.workflows.push("candidate search -> exact stock -> 30 bars -> MA off -> OHLC tooltip");
    await page.goto(`${base}/#candidates`);
    await ready("candidates");
    await page.locator("#candidate-risk").selectOption("blocked");
    assert.equal(await page.locator("#candidate-table tbody tr").count(), 1);
    await page.locator("#candidate-risk").selectOption("");
    await page.locator("#candidate-sort").selectOption("asc");
    assert.match(await page.locator("#candidate-table tbody tr").first().innerText(), /DEMO08/);
    await page.locator("[data-sort]").click();
    assert.equal(await page.locator("#candidate-table tbody tr").first().locator(".num strong").innerText(), "42");
    report.workflows.push("candidate risk and both sort directions");
    await page.goto(`${base}/#signals`);
    await ready("signals");
    await page.locator("#signal-filter").selectOption("unknown_delivery");
    assert.equal(await page.locator(".signal-row").count(), 1);
    assert.match(await page.locator(".signal-row").innerText(), /unknown_delivery/);
    report.workflows.push("unknown_delivery filter");
    await page.goto(`${base}/#performance`);
    await ready("performance");
    for (const horizon of [1, 3, 5, 10]) {
      await page.locator(`[data-horizon="${horizon}"]`).click();
      await ready("performance");
      assert.match(await page.locator(`[data-horizon="${horizon}"]`).getAttribute("class"), /active/);
    }
    report.workflows.push("all T+ horizons");
    await page.goto(`${base}/#settings`);
    await ready("settings");
    assert.equal(await page.locator('#content input:not([disabled])').count(), 0);
    await page.locator("#refresh").click();
    await ready("settings");
    report.workflows.push("settings read-only and refresh");
    if (process.env.WEB_EVIDENCE_FIXTURE === "1") {
      await page.goto(`${base}/#stock/600000`);
      await ready("stock");
      assert.match(await page.locator(".announcement").innerText(), /合成结构夹具公告/);
      assert.match(await page.locator(".evidence-coverage").innerText(), /财务因子 1\/5/);
      assert.match(await page.locator(".evidence-coverage").innerText(), /风险证据 1\/4/);
      await page.screenshot({path: path.join(output, `${width}-evidence-stock.png`), fullPage: true});
      report.workflows.push("current plugin schema -> bound financial/risk evidence and synthetic announcement");
    }
    await page.route("**/api/overview", route => route.fulfill({
      contentType: "application/json", body: JSON.stringify({meta: {status: "unavailable", reason: "database_missing"}, data: null}),
    }));
    await page.goto(`${base}/#overview`);
    await page.locator("#content .empty").waitFor();
    assert.match(await page.locator("#content").innerText(), /database_missing/);
    assert.equal(await page.locator("canvas").count(), 0);
    report.workflows.push("unavailable state clears stale data");
    await page.unroute("**/api/overview");
    }
  } catch (error) {
    report.errors.push(error.stack);
  } finally {
    await browser.close();
    fs.writeFileSync(path.join(output, "report.json"), JSON.stringify(report, null, 2));
  }
  console.log(JSON.stringify(report, null, 2));
  process.exitCode = report.errors.length ? 1 : 0;
})().catch(error => {console.error(error); process.exitCode = 1;});

/*
 * Responsive browser acceptance for the protected dashboard.
 * Usage:
 *   railway variable list ... --json | NODE_PATH="$(npm root -g)" node tests/verify_dashboard_background_responsive.js
 */

"use strict";

const fs = require("fs");
const { chromium } = require("playwright");

const variables = JSON.parse(fs.readFileSync(0, "utf8"));
const accessToken = variables.TONMEN_WEB_TOKEN;
if (!accessToken) throw new Error("TopMen access token unavailable.");

const cases = [
  ["mobile-390x844", 390, 844],
  ["tablet-768x1024", 768, 1024],
  ["laptop-1280x720", 1280, 720],
  ["desktop-1440x900", 1440, 900],
];

(async () => {
  const browser = await chromium.launch({ executablePath: "/usr/bin/chromium", headless: true });
  const results = [];
  try {
    for (const [name, width, height] of cases) {
      const context = await browser.newContext({ viewport: { width, height }, deviceScaleFactor: 1 });
      const page = await context.newPage();
      const errors = [];
      page.on("pageerror", error => errors.push(error.message));
      await page.goto(`https://topmen.app/?responsive=${name}`, { waitUntil: "networkidle", timeout: 30000 });
      const login = await page.evaluate(async token => {
        const response = await fetch("/login", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ token }),
        });
        return { status: response.status, body: await response.json() };
      }, accessToken);
      if (login.status !== 200 || !login.body.ok) throw new Error(`Dashboard login failed at ${name}`);
      await page.goto(`https://topmen.app/?responsive=${name}`, { waitUntil: "networkidle", timeout: 30000 });
      await page.waitForSelector("#provinceOptions input", { timeout: 10000 });
      const metrics = await page.evaluate(() => {
        const dashboard = document.querySelector(".dashboard");
        const logPanel = document.querySelector(".log-panel");
        const start = document.querySelector("#btnStart").getBoundingClientRect();
        const stop = document.querySelector("#btnStop").getBoundingClientRect();
        const after = getComputedStyle(dashboard, "::after");
        const before = getComputedStyle(dashboard, "::before");
        return {
          scrollWidth: document.documentElement.scrollWidth,
          scrollHeight: document.documentElement.scrollHeight,
          clientWidth: document.documentElement.clientWidth,
          clientHeight: document.documentElement.clientHeight,
          dashboardHeight: Math.round(dashboard.getBoundingClientRect().height),
          logHeight: Math.round(logPanel.getBoundingClientRect().height),
          buttonVisible: start.top >= 0 && start.bottom <= innerHeight && stop.top >= 0 && stop.bottom <= innerHeight,
          artworkBackgroundSize: after.backgroundSize,
          fillBackgroundSize: before.backgroundSize,
        };
      });
      if (metrics.scrollWidth > metrics.clientWidth || metrics.scrollHeight > metrics.clientHeight) {
        throw new Error(`Unexpected viewport overflow at ${name}: ${JSON.stringify(metrics)}`);
      }
      if (!metrics.buttonVisible || metrics.logHeight < 100) {
        throw new Error(`Console controls or log panel not usable at ${name}: ${JSON.stringify(metrics)}`);
      }
      if (!metrics.artworkBackgroundSize.includes("contain") || !metrics.fillBackgroundSize.includes("cover")) {
        throw new Error(`Background layer sizing regression at ${name}: ${JSON.stringify(metrics)}`);
      }
      const screenshot = `/home/ubuntu/responsive-dashboard-${name}.png`;
      await page.screenshot({ path: screenshot });
      results.push({ name, width, height, screenshot, metrics, pageErrors: errors });
      await context.close();
    }
  } finally {
    await browser.close();
  }
  console.log(JSON.stringify(results, null, 2));
})().catch(error => {
  console.error(error.stack || error);
  process.exit(1);
});

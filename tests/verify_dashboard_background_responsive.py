"""Browser acceptance for responsive protected dashboard backgrounds.

Usage:
railway variable list ... --json | python3 tests/verify_dashboard_background_responsive.py
"""

import json
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

variables = json.load(sys.stdin)
access_token = variables.get("TONMEN_WEB_TOKEN")
if not isinstance(access_token, str) or not access_token:
    raise SystemExit("TopMen access token unavailable.")

cases = (
    ("mobile-390x844", 390, 844),
    ("tablet-768x1024", 768, 1024),
    ("laptop-1280x720", 1280, 720),
    ("desktop-1440x900", 1440, 900),
)
results = []

with sync_playwright() as playwright:
    browser = playwright.chromium.launch(executable_path="/usr/bin/chromium", headless=True)
    try:
        for name, width, height in cases:
            context = browser.new_context(viewport={"width": width, "height": height}, device_scale_factor=1)
            page = context.new_page()
            page_errors = []
            page.on("pageerror", lambda error: page_errors.append(str(error)))
            page.goto(f"https://topmen.app/?responsive={name}", wait_until="networkidle", timeout=30_000)
            login = page.evaluate(
                """async token => {
                    const response = await fetch('/login', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({token}),
                    });
                    return {status: response.status, body: await response.json()};
                }""",
                access_token,
            )
            if login["status"] != 200 or not login["body"].get("ok"):
                raise RuntimeError(f"Dashboard login failed at {name}")
            page.goto(f"https://topmen.app/?responsive={name}", wait_until="networkidle", timeout=30_000)
            page.wait_for_selector("#provinceOptions input", timeout=10_000)
            metrics = page.evaluate(
                """() => {
                    const dashboard = document.querySelector('.dashboard');
                    const logPanel = document.querySelector('.log-panel');
                    const start = document.querySelector('#btnStart').getBoundingClientRect();
                    const stop = document.querySelector('#btnStop').getBoundingClientRect();
                    const artwork = getComputedStyle(dashboard, '::after');
                    const fill = getComputedStyle(dashboard, '::before');
                    return {
                        scrollWidth: document.documentElement.scrollWidth,
                        scrollHeight: document.documentElement.scrollHeight,
                        clientWidth: document.documentElement.clientWidth,
                        clientHeight: document.documentElement.clientHeight,
                        dashboardHeight: Math.round(dashboard.getBoundingClientRect().height),
                        logHeight: Math.round(logPanel.getBoundingClientRect().height),
                        buttonVisible: start.top >= 0 && start.bottom <= innerHeight && stop.top >= 0 && stop.bottom <= innerHeight,
                        artworkBackgroundSize: artwork.backgroundSize,
                        fillBackgroundSize: fill.backgroundSize,
                    };
                }"""
            )
            if metrics["scrollWidth"] > metrics["clientWidth"] or metrics["scrollHeight"] > metrics["clientHeight"]:
                raise RuntimeError(f"Unexpected viewport overflow at {name}: {metrics}")
            if not metrics["buttonVisible"] or metrics["logHeight"] < 100:
                raise RuntimeError(f"Console controls or log panel not usable at {name}: {metrics}")
            if "contain" not in metrics["artworkBackgroundSize"] or "cover" not in metrics["fillBackgroundSize"]:
                raise RuntimeError(f"Background layer sizing regression at {name}: {metrics}")
            screenshot = Path(f"/home/ubuntu/responsive-dashboard-{name}.png")
            page.screenshot(path=str(screenshot))
            results.append({
                "name": name,
                "width": width,
                "height": height,
                "screenshot": str(screenshot),
                "metrics": metrics,
                "pageErrors": page_errors,
            })
            context.close()
    finally:
        browser.close()

print(json.dumps(results, ensure_ascii=False, indent=2))

// Headless checks against generated pages; pass the generation directory as argv[2].
const {chromium} = require('playwright');
const {pathToFileURL} = require('node:url');
const path = require('node:path');
const fs = require('node:fs');
const assert = require('node:assert/strict');

(async () => {
  const directory = path.resolve(process.argv[2] || 'reports');
  const evidence = path.resolve(process.argv[3] || 'dist/browser-evidence');
  fs.mkdirSync(evidence, {recursive: true});
  const browser = await chromium.launch({headless: true});
  const errors = [];
  const results = [];
  try {
    for (const width of [1440, 390]) {
      const page = await browser.newPage({viewport: {width, height: 950}});
      page.on('pageerror', error => errors.push(error.message));
      await page.route(/^https?:/, route => route.abort());
      await page.goto(pathToFileURL(path.join(directory, 'svu_daily.html')).href);
      await page.waitForSelector('#chart svg');
      await page.click('#language');
      await page.waitForFunction(() => document.documentElement.lang === 'en');
      assert.match(await page.locator('h1').textContent(), /Cross-asset/);
      await page.waitForTimeout(100);
      assert(!/[\u4e00-\u9fff]/.test(await page.locator('body').innerText()), 'Chinese text in English daily view');
      await page.click('#language');
      await page.waitForFunction(() => document.documentElement.lang === 'zh-CN');
      await page.check('input[name=mode][value=official]');
      await page.waitForFunction(() => PLOT && plotMode === 'official');
      assert(await page.evaluate(() => PLOT.series.SVU.every(x => x === 100)));
      const before = await page.inputValue('#startSlider');
      await page.evaluate(() => {
        const slider = document.getElementById('startSlider');
        slider.value = Math.floor((+slider.min + +slider.max) / 2);
        slider.dispatchEvent(new Event('input', {bubbles: true}));
      });
      await page.waitForFunction(() => PLOT && PLOT.dates[0] >= document.getElementById('startDate').value);
      assert.notEqual(await page.inputValue('#startSlider'), before);
      await page.click('#none');
      await page.waitForFunction(() => PLOT && PLOT.names.length === 1);
      await page.click('#reset');
      await page.waitForFunction(() => PLOT && PLOT.names.length > 1);
      // Empty official window followed by a valid one must retain tooltip behavior.
      await page.evaluate(() => {
        window.savedPrices = DATA.prices[DATA.official_assets[0]].slice(0, 2);
        DATA.prices[DATA.official_assets[0]][0] = null;
        DATA.prices[DATA.official_assets[0]][1] = null;
        startIdx = 0; endIdx = 1; draw();
      });
      await page.evaluate(() => {
        DATA.prices[DATA.official_assets[0]].splice(0, 2, ...window.savedPrices);
        startIdx = 0; endIdx = DATA.dates.length - 1; draw();
      });
      await page.locator('#chart').scrollIntoViewIfNeeded();
      const box = await page.locator('#chart svg').boundingBox();
      await page.mouse.move(box.x + Math.min(box.width / 2, width / 2), box.y + 150);
      await page.waitForTimeout(100);
      assert(await page.locator('#tip').count(), 'tooltip disappeared after empty window');
      assert(await page.locator('#tip').isVisible(), 'tooltip does not recover after empty window');
      await page.screenshot({path: path.join(evidence, `daily-${width}.png`), fullPage: true});
      assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 2), 'page overflows viewport');
      for (const domain of ['currency', 'energy', 'monetary_hedge', 'equity_index']) {
        await page.goto(pathToFileURL(path.join(directory, 'domains', domain + '.html')).href);
        await page.waitForSelector('#domainChart svg');
        await page.waitForSelector('#chart svg');
        await page.click('#language');
        await page.waitForFunction(() => document.documentElement.lang === 'en');
        assert.match(await page.locator('#pageTitle').textContent(), /relative view/);
        for (const action of ['#none', '#all', '#reset']) {
          await page.click(action);
          assert(!/[\u4e00-\u9fff]/.test(await page.locator('body').innerText()), 'Chinese text after English domain interaction');
        }
        assert.equal(await page.locator('#looSummary').evaluate(el => el.closest('table').querySelectorAll('th')[1].textContent), 'Leave-one-out ICATI end');
        assert.equal(await page.locator('#diagnosticCards .card span').first().textContent(), 'ICATI start');
        await page.reload();
        assert.equal(await page.locator('html').getAttribute('lang'), 'en');
        await page.click('#language');
        await page.click('#none');
        assert.equal(await page.locator('#toggles input:checked').count(), 0);
        await page.click('#reset');
        assert(await page.locator('#toggles input:checked').count());
        assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 2), domain + ' overflows viewport');
        if (domain === 'currency') await page.screenshot({path: path.join(evidence, `currency-${width}.png`), fullPage: true});
      }
      results.push({width, status: 'ok'});
      await page.close();
    }
    assert.deepEqual(errors, [], 'browser JavaScript errors');
    fs.writeFileSync(path.join(evidence, 'result.json'), JSON.stringify({results, errors}, null, 2));
    console.log(JSON.stringify({results, errors}));
  } finally {
    await browser.close();
  }
})().catch(error => {console.error(error.stack); process.exitCode = 1;});

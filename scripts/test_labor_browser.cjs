const {chromium}=require('playwright');
const {pathToFileURL}=require('url');
const path=require('path');
const assert=require('node:assert/strict');

(async()=>{
  const browser=await chromium.launch({headless:true});
  try {
    for(const width of [1440,390]){
      const page=await browser.newPage({viewport:{width,height:900}}), errors=[];
      page.on('pageerror',e=>errors.push(e.message));
      await page.goto(pathToFileURL(path.resolve(process.argv[2]||'reports/labor.html')).href);
      assert.ok(await page.locator('#annual-country-select option').count()>100);
      assert.equal(await page.locator('#country-comparison svg').count(),1);
      assert.equal(await page.locator('#single-country-details').getAttribute('open'),null);
      for(const basis of ['raw','currency','svu']){
        await page.locator('#comparison-basis').selectOption(basis);
        assert.equal(await page.locator('#country-comparison svg').count(),1);
        assert.equal(await page.locator('#country-comparison svg').getAttribute('data-comparison-basis'),basis);
        assert.equal(await page.locator('#country-comparison path[data-country]').count(),2);
      }
      await page.locator('#single-country-summary').click();
      await page.locator('#countries-none').click();
      for(const code of ['USA','RUS','HKG'])await page.locator('input[data-country="'+code+'"]').check();
      assert.equal(await page.locator('#country-comparison path[data-country="HKG"]').count(),0);
      assert.ok(await page.locator('[data-country-status="HKG"]').evaluate(el=>el.classList.contains('warning')));
      assert.notEqual(await page.locator('#country-comparison path[data-country="USA"]').getAttribute('d'),await page.locator('#country-comparison path[data-country="RUS"]').getAttribute('d'));
      const usColor=await page.locator('#country-comparison path[data-country="USA"]').getAttribute('stroke');
      await page.locator('input[data-country="JPN"]').check();
      assert.equal(await page.locator('#country-comparison path[data-country="USA"]').getAttribute('stroke'),usColor);
      await page.locator('#comparison-basis').selectOption('raw');
      assert.equal(await page.locator('#country-comparison path[data-country="HKG"]').count(),1);
      assert.notEqual(await page.locator('#country-comparison path[data-country="HKG"]').getAttribute('d'),await page.locator('#country-comparison path[data-country="USA"]').getAttribute('d'));
      await page.locator('#comparison-basis').selectOption('svu');
      await page.locator('#countries-none').click();
      for(const code of ['CHN','USA'])await page.locator('input[data-country="'+code+'"]').check();
      await page.locator('#country-search').fill('JPN');
      await page.locator('input[data-country="JPN"]').check();
      assert.equal(await page.locator('svg[data-comparison-basis="svu"] path[data-country]').count(),3);
      await page.locator('input[data-country="JPN"]').uncheck();
      assert.equal(await page.locator('svg[data-comparison-basis="svu"] path[data-country]').count(),2);
      await page.locator('#country-search').fill('');
      await page.locator('#countries-none').click();
      assert.equal(await page.locator('#country-comparison svg').count(),0);
      for(const code of ['CHN','USA'])await page.locator('input[data-country="'+code+'"]').check();
      await page.locator('#language').click();
      assert.equal(await page.locator('#annual-title').textContent(),'Labour and earnings by country');
      assert.equal(await page.locator('#title').textContent(),'United States monthly tracker');
      assert.ok(!/[\u3400-\u9fff]/u.test(await page.locator('body').innerText()),'Chinese text in English view');
      await page.locator('#dataset').selectOption('monthly');
      await page.locator('[data-years="5"]').click();
      assert.ok(await page.locator('#rows tr').count()>0);
      await page.locator('#dataset').selectOption('annual');
      await page.locator('#annual-country-select').selectOption('CHN');
      await page.locator('#annual-metric-select').selectOption('hourly');
      assert.equal(await page.locator('#annual-latest').textContent(),'—');
      assert.equal(await page.locator('#annual-rows tr').count(),0);
      await page.locator('#annual-metric-select').selectOption('monthly');
      assert.ok(await page.locator('#annual-rows tr').count()>0);
      assert.ok(await page.locator('#annual-benchmark strong').count()===2);
      await page.locator('#annual-benchmark svg').focus();
      await page.keyboard.press('ArrowRight');
      assert.ok(await page.locator('#annual-benchmark .chart-tip').isVisible());
      await page.locator('#none').click();
      assert.equal(await page.locator('#annual-benchmark svg').isVisible(),false);
      await page.locator('#all').click();
      assert.equal(await page.locator('#annual-benchmark svg').isVisible(),true);
      await page.locator('#annual-country-select').selectOption('USA');
      const fixedAmount=await page.locator('#annual-benchmark strong').first().textContent();
      await page.locator('[data-years="3"]').click();
      assert.equal(await page.locator('#annual-benchmark strong').first().textContent(),fixedAmount);
      assert.ok(await page.locator('#annual-rows tr').count()<=3);
      await page.locator('#reset').click();
      // Exercise every country's default survey, including missing indicators.
      const countries=await page.locator('#annual-country-select option').evaluateAll(os=>os.map(o=>o.value));
      for(const country of countries){
        await page.locator('#annual-country-select').selectOption(country);
        const available=await page.locator('#annual-survey-select option').count();
        assert.equal(await page.locator('#annual-rows tr').count()>0,available>0);
      }
      await page.locator('#annual-country-select').selectOption('USA');
      await page.locator('#annual-metric-select').selectOption('monthly');
      assert.ok(await page.locator('#annual-rows tr').count()>0);
      await page.locator('#language').click();
      assert.equal(await page.locator('#annual-title').textContent(),'各国劳动与收入');
      assert.ok(!(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth)),'Horizontal page overflow');
      await page.screenshot({path:'dist/labor-benchmarks-'+width+'.png',fullPage:true});
      const target=await page.locator('#home').getAttribute('href');
      assert.ok(target&&target.includes('generations/'));
      await page.locator('#home').click();
      await page.locator('[data-i18n="laborLink"]').waitFor({state:'visible'});
      await page.locator('[data-i18n="laborLink"]').click();
      await page.locator('#dataset').waitFor({state:'visible'});
      for(const module of ['currency','energy','monetary_hedge','equity_index']){
        await page.locator('#nav-'+module).click();
        await page.locator('#laborLink').waitFor({state:'visible'});
        await page.locator('#laborLink').click();
        await page.locator('#dataset').waitFor({state:'visible'});
      }
      await page.goto(pathToFileURL(path.resolve('reports/index.html')).href);
      await page.locator('[data-i18n="laborLink"]').waitFor({state:'visible'});
      await page.locator('[data-i18n="laborLink"]').click();
      await page.locator('#dataset').waitFor({state:'visible'});
      assert.deepEqual(errors,[]);
      await page.close();
    }
    console.log('Country selection, missing data, bilingual desktop/mobile layout and console checks passed');
  } finally {await browser.close()}
})().catch(e=>{console.error(e);process.exit(1)});

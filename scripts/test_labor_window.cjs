// Offline regression checks; no downloaded country data or browser required.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const elements = new Map();
const context = vm.createContext({document: {getElementById(id) {
  if (!elements.has(id)) elements.set(id, {value: id === 'dataset' ? 'annual' : ''});
  return elements.get(id);
}}});
vm.runInContext(fs.readFileSync(path.join(__dirname, '../validation/labor_controls.js'), 'utf8') + '\nlet language="en";', context);
function run(code) { return vm.runInContext(code, context); }
function sync(years, key='annual|monthly') {
  run(`syncLaborWindow(${JSON.stringify(years.map(year=>({year})))}, 'year', ${JSON.stringify(key)})`);
}
function window() {return run('laborWindow.start+":"+laborWindow.end');}
sync(['1994','2023','2024','2025']);
run('laborWindow.start="2023";laborWindow.end="2025"');
sync(['1990','2023','2024','2025']); // Adding older country history.
assert.equal(window(),'2023:2025');
sync(['2023','2025']); // Missing intermediate year must not shift the window.
assert.equal(window(),'2023:2025');
sync([]);
assert.equal(window(),'2023:2025');
assert.equal(elements.get('startDate').disabled,true);
sync(['2000','2023','2025']);
assert.equal(window(),'2023:2025');
sync(['2024','2025']); // Only the excluded bound is clamped.
assert.equal(window(),'2024:2025');
run('laborWindow.start="2023";laborWindow.end="2024"');
sync(['2020','2022','2025']);
assert.equal(window(),'2023:2024');
assert.equal(elements.get('startSlider').value,1);
assert.equal(elements.get('endSlider').value,2);
run('laborWindow.key=""'); // Explicit all-history/reset still works.
sync(['1994','2023','2025']);
assert.equal(window(),'1994:2025');
sync(['2010','2020'],'annual|hourly');
assert.equal(window(),'2010:2020');
console.log('Labour window regression checks passed');

const fs = require("fs");

if (!process.env.INPUT_TOKEN) {
  throw new Error("github.token input is missing");
}

const dynamicSecret = `generated-${process.env.INPUT_VALUE}-secret`;
console.log(`::add-mask::${dynamicSecret}`);
console.log(`::set-output name=legacy::legacy-${process.env.INPUT_VALUE}`);
console.log(`::set-output name=legacy-secret::${dynamicSecret}`);
console.log(`::save-state name=legacy::${process.env.INPUT_VALUE}`);
console.log(`dynamic-secret=${dynamicSecret}`);

fs.appendFileSync(
  process.env.GITHUB_OUTPUT,
  `result=${process.env.INPUT_VALUE}\ndynamic-secret=${dynamicSecret}\ntoken=${process.env.INPUT_TOKEN}\n`,
);
fs.appendFileSync(
  process.env.GITHUB_STATE,
  `saved=${process.env.INPUT_VALUE}\n`,
);
fs.appendFileSync(
  process.env.GITHUB_STEP_SUMMARY,
  `JavaScript summary ${process.env.INPUT_VALUE}\nsummary-token=${process.env.INPUT_TOKEN}\ndynamic-summary-secret=${dynamicSecret}\n`,
);
console.log(`javascript-${process.env.INPUT_VALUE}`);
console.log("javascript-token-available");

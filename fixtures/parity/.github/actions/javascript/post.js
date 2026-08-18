const fs = require("fs");

fs.appendFileSync(
  process.env.GITHUB_STEP_SUMMARY,
  `Post summary ${process.env.STATE_saved}\n`,
);
console.log(`post-${process.env.STATE_saved}`);
console.log(`legacy-post-${process.env.STATE_legacy}`);

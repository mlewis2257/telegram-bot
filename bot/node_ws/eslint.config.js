import globals from "globals";

export default [
  // CommonJS files
  //   {
  //     files: ["**/*.js"],
  //     languageOptions: {
  //       ecmaVersion: "latest",
  //       sourceType: "commonjs",
  //       globals: { ...globals.node, ...globals.browser },
  //     },
  //   },

  // If you have ESM files like *.mjs or a directory you know is ESM:
  {
    files: ["**/*.mjs"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: { ...globals.node, ...globals.browser },
    },
  },
];

module.exports = {
  parser: "@typescript-eslint/parser",
  parserOptions: {
    project: "tsconfig.json",
    tsconfigRootDir: __dirname,
    sourceType: "module",
  },
  plugins: ["@typescript-eslint/eslint-plugin", "@stylistic"],
  extends: ["plugin:@typescript-eslint/recommended", "plugin:prettier/recommended"],
  root: true,
  env: {
    node: true,
    jest: true,
  },
  ignorePatterns: [".eslintrc.js"],
  rules: {
    "@typescript-eslint/naming-convention": [
      "warn",
      {
        selector: "default",
        format: ["camelCase"],
        leadingUnderscore: "allow",
        trailingUnderscore: "allow",
      },
      {
        selector: "typeLike",
        format: ["PascalCase"],
      },
      {
        selector: "enumMember",
        format: ["camelCase", "UPPER_CASE"],
      },
      {
        selector: "objectLiteralProperty",
        format: ["camelCase", "snake_case"],
      },
      {
        selector: "variableLike",
        format: ["camelCase"],
        leadingUnderscore: "allow",
        trailingUnderscore: "allow",
      },
      {
        selector: "variable",
        modifiers: ["const", "global"],
        format: ["PascalCase", "UPPER_CASE"],
      },
      {
        selector: "variable",
        modifiers: ["const"],
        format: ["camelCase", "PascalCase", "UPPER_CASE"],
      },
      {
        selector: ["classProperty", "parameter", "variable"],
        types: ["boolean"],
        format: ["PascalCase"],
        prefix: [
          "is",
          "are",
          "was",
          "were",
          "shall",
          "should",
          "has",
          "have",
          "had",
          "can",
          "could",
          "does",
          "do",
          "did",
          "done",
          "will",
          "would",
          "enable",
          "with",
        ],
        leadingUnderscore: "allow",
      },
      {
        selector: ["classProperty"],
        modifiers: ["static"],
        format: ["PascalCase", "UPPER_CASE"],
        leadingUnderscore: "allow",
      },
    ],
    "@typescript-eslint/explicit-module-boundary-types": "off",
    "@typescript-eslint/no-explicit-any": ["warn", { ignoreRestArgs: true }],
    "@typescript-eslint/ban-types": "off",
    "@typescript-eslint/explicit-function-return-type": [
      "warn",
      {
        allowTypedFunctionExpressions: true,
        allowExpressions: true,
      },
    ],
    "@stylistic/lines-between-class-members": [
      "warn",
      {
        enforce: [
          { blankLine: "always", prev: "method", next: "method" },
          { blankLine: "always", prev: "field", next: "method" },
        ],
      },
      { exceptAfterSingleLine: true },
    ],
    "@typescript-eslint/no-unused-vars": [
      "warn",
      {
        argsIgnorePattern: "^_",
        varsIgnorePattern: "^_",
        caughtErrorsIgnorePattern: "^_",
      },
    ],
    curly: ["warn", "all"],
  },
};

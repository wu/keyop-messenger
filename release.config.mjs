/**
 * Semantic Release configuration for keyop-messenger.
 *
 * Convention → SemVer mapping:
 *   feat:              minor bump  (0.1.0 → 0.2.0)
 *   fix: / perf:       patch bump  (0.1.0 → 0.1.1)
 *   BREAKING CHANGE:   major bump  (0.1.0 → 1.0.0)
 *
 * Tags are written WITHOUT a "v" prefix to match the existing scheme
 * (0.0.1, 0.0.2, …).
 *
 * Required GitHub Actions secret:  GITHUB_TOKEN  (provided automatically)
 */

export default {
  // Match the existing bare-semver tag format (no "v" prefix).
  tagFormat: "${version}",

  branches: ["main"],

  plugins: [
    // 1. Analyse commits using Conventional Commits.
    [
      "@semantic-release/commit-analyzer",
      {
        preset: "conventionalcommits",
        releaseRules: [
          // Treat any "feat" as a minor bump (default), "fix"/"perf" as patch.
          // Explicitly map extra types used in this repo so they don't block a
          // release when they appear alongside a feat/fix commit.
          {type: "ci", release: false},
          {type: "conf", release: false},
          {type: "docs", release: false},
          {type: "chore", release: false},
          {type: "style", release: false},
          {type: "test", release: false},
          {type: "build", release: false},
          {type: "refactor", release: "patch"},
        ],
        parserOpts: {
          noteKeywords: ["BREAKING CHANGE", "BREAKING CHANGES"],
        },
      },
    ],

    // 2. Generate CHANGELOG.md
    [
      "@semantic-release/release-notes-generator",
      {
        preset: "conventionalcommits",
        presetConfig: {
          types: [
            {type: "feat", section: "Features"},
            {type: "fix", section: "Bug Fixes"},
            {type: "perf", section: "Performance Improvements"},
            {type: "refactor", section: "Refactoring"},
            {type: "docs", section: "Documentation", hidden: true},
            {type: "chore", section: "Chores", hidden: true},
            {type: "ci", section: "CI/CD", hidden: true},
          ],
        },
      },
    ],

    // 3. Write / update CHANGELOG.md in the repo.
    [
      "@semantic-release/changelog",
      {
        changelogFile: "CHANGELOG.md",
      },
    ],

    // 4. Commit the updated CHANGELOG.md back to main.
    [
      "@semantic-release/git",
      {
        assets: ["CHANGELOG.md"],
        message:
            "chore(release): ${nextRelease.version} [skip ci]\n\n${nextRelease.notes}",
      },
    ],

    // 5. Create the GitHub release (tag + release notes).
    //    Binary assets are uploaded by the separate build-release job which
    //    triggers on the "release: published" event emitted here.
    [
      "@semantic-release/github",
      {
        // Assets are attached by the matrix build job; nothing to add here.
        assets: [],
      },
    ],
  ],
};

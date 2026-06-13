/**
 * Semantic Release configuration for keyop-messenger.
 *
 * Convention → SemVer mapping:
 *   feat:              minor bump  (1.1.0 → 1.2.0)
 *   fix: / perf:       patch bump  (1.1.0 → 1.1.1)
 *   feat! / BREAKING:  minor bump  (1.1.0 → 1.2.0)  ← see below
 *
 * Major bumps are intentionally disabled. Go's module system requires a
 * versioned import path (/v2, /v3, …) for every major version ≥ 2, which
 * means each major bump would force a rename of the module path in this
 * repo AND in every consumer. Since keyop-messenger has a small set of
 * known consumers (currently just keyop) and breaking changes are
 * surfaced clearly in the changelog and commit messages, the rename cost
 * outweighs the import-path safety signal. We stay on v1.x.x and use
 * `feat!:` / `BREAKING CHANGE:` to signal API breaks in human-readable
 * release notes only.
 *
 * If you genuinely need to publish a versioned import path (e.g. for
 * external consumers who must pin against incompatible majors), remove
 * the `{breaking: true, release: "minor"}` rule below and follow Go's
 * module-versioning playbook (rename the module path in go.mod, update
 * every internal import, regenerate proto code, update every consumer).
 *
 * Tags are written with a "v" prefix (v1.4.0, v1.4.1, …).
 *
 * Required GitHub Actions secret:  GITHUB_TOKEN  (provided automatically)
 */

export default {
  // Tags are written with a "v" prefix (v0.5.0, v0.5.1, …).
  tagFormat: "v${version}",

  branches: ["main"],

  plugins: [
    // 1. Analyse commits using Conventional Commits.
    [
      "@semantic-release/commit-analyzer",
      {
        preset: "conventionalcommits",
        releaseRules: [
          // Force breaking-change markers (footer `BREAKING CHANGE:` or
          // the `feat!:` / `fix!:` `!` suffix) to a MINOR bump instead of
          // the conventionalcommits default (major). This must come first
          // so it wins over any later rule that might match the same
          // commit. See the file header for the rationale.
          {breaking: true, release: "minor"},

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

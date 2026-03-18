#!/bin/bash
# Clarity Dashboard — GitHub Setup Script
# Run this once from your clarity-dashboard folder: bash setup_github.sh

set -e
cd "$(dirname "$0")"

echo ""
echo "🚀 Clarity Dashboard — GitHub Setup"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── Check for gh CLI ─────────────────────────────────────────────────────────
if ! command -v gh &> /dev/null; then
  echo ""
  echo "Installing GitHub CLI (gh)..."
  brew install gh
fi

# ── Authenticate if needed ───────────────────────────────────────────────────
if ! gh auth status &> /dev/null; then
  echo ""
  echo "📋 You need to log in to GitHub first."
  gh auth login
fi

# ── Create the repo ──────────────────────────────────────────────────────────
REPO_NAME="clarity-dashboard"
echo ""
echo "📦 Creating private GitHub repo: $REPO_NAME"
gh repo create "$REPO_NAME" --private --source=. --remote=origin --push
echo "✅ Repo created and code pushed!"

# ── Add secrets ──────────────────────────────────────────────────────────────
echo ""
echo "🔐 Adding GitHub Secrets..."
echo "   (You'll be prompted to paste each one)"

echo ""
echo "── SECRET 1 of 4: CONFIG_JSON ──────────────────────────────────────────"
echo "   Paste the full contents of your config.json, then press ENTER + Ctrl-D"
gh secret set CONFIG_JSON

echo ""
echo "── SECRET 2 of 4: ASC_KEY_SHIFT ────────────────────────────────────────"
echo "   Paste the full contents of shift.p8, then press ENTER + Ctrl-D"
gh secret set ASC_KEY_SHIFT

echo ""
echo "── SECRET 3 of 4: ASC_KEY_TODAYS_FRONT_PAGES ───────────────────────────"
echo "   Paste the full contents of frontPages.p8, then press ENTER + Ctrl-D"
gh secret set ASC_KEY_TODAYS_FRONT_PAGES

echo ""
echo "── SECRET 4 of 4: ASC_KEY_P3 ───────────────────────────────────────────"
echo "   Paste the full contents of p3.p8, then press ENTER + Ctrl-D"
gh secret set ASC_KEY_P3

echo ""
echo "✅ All secrets added!"

# ── Enable GitHub Pages ──────────────────────────────────────────────────────
echo ""
echo "🌐 Enabling GitHub Pages..."
GITHUB_USER=$(gh api user --jq '.login')
gh api \
  --method POST \
  -H "Accept: application/vnd.github+json" \
  "/repos/$GITHUB_USER/$REPO_NAME/pages" \
  -f source='{"branch":"main","path":"/"}' 2>/dev/null || \
gh api \
  --method PUT \
  -H "Accept: application/vnd.github+json" \
  "/repos/$GITHUB_USER/$REPO_NAME/pages" \
  -f source='{"branch":"main","path":"/"}' 2>/dev/null || \
echo "⚠️  Pages setup may need a moment — check repo Settings → Pages if needed"

echo ""
echo "🎉 All done!"
echo ""
echo "   Your dashboard will be live at:"
echo "   https://$GITHUB_USER.github.io/$REPO_NAME/clarity_dashboard.html"
echo ""
echo "   To run the workflow now (fresh data):"
echo "   gh workflow run update-dashboard.yml --repo $GITHUB_USER/$REPO_NAME"
echo ""

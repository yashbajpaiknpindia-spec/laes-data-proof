from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")
DASHBOARD_JS = (ROOT / "static" / "app.js").read_text(encoding="utf-8")


class DashboardDomRegressionTests(unittest.TestCase):
    """Guard against duplicate page trees and broken dashboard navigation."""

    def test_template_has_one_document_shell(self) -> None:
        self.assertEqual(len(re.findall(r"<!doctype\s+html>", TEMPLATE, re.I)), 1)
        self.assertEqual(len(re.findall(r"<html\b", TEMPLATE, re.I)), 1)
        self.assertEqual(len(re.findall(r"<body\b", TEMPLATE, re.I)), 1)

    def test_every_element_id_is_unique(self) -> None:
        ids = re.findall(r'\bid="([^"]+)"', TEMPLATE)
        duplicates = sorted({value for value in ids if ids.count(value) > 1})
        self.assertEqual(duplicates, [], f"Duplicate DOM IDs: {duplicates}")

    def test_each_dashboard_page_exists_once(self) -> None:
        expected = {
            "page-overview", "page-traders", "page-positions",
            "page-opportunities", "page-paper", "page-market", "page-database",
        }
        pages = re.findall(r'<section class="page(?: active)?" id="(page-[^"]+)"', TEMPLATE)
        self.assertEqual(set(pages), expected)
        self.assertEqual(len(pages), len(expected))

    def test_navigation_targets_resolve_to_pages(self) -> None:
        page_ids = set(re.findall(r'<section class="page(?: active)?" id="(page-[^"]+)"', TEMPLATE))
        nav_targets = set(re.findall(r'data-page="([^"]+)"', TEMPLATE))
        goto_targets = set(re.findall(r'data-goto="([^"]+)"', TEMPLATE))
        self.assertEqual(nav_targets, {name.removeprefix("page-") for name in page_ids})
        self.assertTrue(goto_targets.issubset({name.removeprefix("page-") for name in page_ids}))

    def test_live_paper_toolbar_is_scoped_to_paper_page(self) -> None:
        paper_start = TEMPLATE.index('<section class="page" id="page-paper">')
        paper_end = TEMPLATE.index("</section>", paper_start)
        toolbar = TEMPLATE.index('<div class="top-live-strip">')
        self.assertLess(paper_start, toolbar)
        self.assertLess(toolbar, paper_end)

    def test_static_javascript_dom_hooks_exist(self) -> None:
        template_ids = set(re.findall(r'\bid="([^"]+)"', TEMPLATE))
        script_ids = set(re.findall(r'\$\("([^"]+)"\)', DASHBOARD_JS))
        missing = sorted(script_ids - template_ids)
        self.assertEqual(missing, [], f"JavaScript references missing DOM hooks: {missing}")

    def test_click_navigation_has_single_delegated_router(self) -> None:
        self.assertEqual(DASHBOARD_JS.count('document.addEventListener("click"'), 1)
        self.assertNotIn("bindPaperTabs", DASHBOARD_JS)

    def test_market_and_system_pages_load_their_data(self) -> None:
        self.assertIn('else if(name==="market"){loadMarket(false);loadMarketUniverse();}', DASHBOARD_JS)
        self.assertIn('else if(name==="database")loadSystem();', DASHBOARD_JS)


if __name__ == "__main__":
    unittest.main()

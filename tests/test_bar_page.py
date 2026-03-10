from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch


class TestBarPage(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch("dash.register_page", lambda *args, **kwargs: None):
            cls.bar = importlib.import_module("pages.bar")
        if not cls.bar._datasets:
            raise unittest.SkipTest("No structured datasets available for bar page tests.")

    def _pick_dataset_with_distribution(self):
        for name, sd in self.bar._datasets.items():
            if self.bar._distribution_variable_options(sd):
                return name, sd
        self.skipTest("No dataset with distribution variables found.")

    def test_distribution_controls_populate(self):
        dataset_name, sd = self._pick_dataset_with_distribution()
        x_col = sd.schema.primary_variables[0].column_name

        outputs = self.bar.update_controls(
            dataset_name,
            x_col,
            None,
            None,
            "distribution",
            None,
            None,
            "percent",
        )

        # Output tuple positions from update_controls callback.
        dist_var_opts = outputs[7]
        dist_var_disabled = outputs[9]
        dist_cat_opts = outputs[10]
        dist_cat_disabled = outputs[12]
        dist_metric_disabled = outputs[14]

        self.assertTrue(dist_var_opts, "Expected distribution variable options.")
        self.assertTrue(dist_cat_opts, "Expected category options for selected distribution.")
        self.assertFalse(dist_var_disabled, "Distribution variable selector should be enabled in distribution mode.")
        self.assertFalse(dist_cat_disabled, "Distribution category selector should be enabled in distribution mode.")
        self.assertFalse(dist_metric_disabled, "Distribution metric selector should be enabled in distribution mode.")

    def test_distribution_mode_count_and_percent_chart(self):
        dataset_name, sd = self._pick_dataset_with_distribution()
        x_col = sd.schema.primary_variables[0].column_name
        context_col = self.bar._context_primary_column(sd, x_col)

        dist_var = self.bar._distribution_variable_options(sd)[0]["value"]
        category = self.bar._distribution_category_options(sd, dist_var)[0]["value"]

        context_value = None
        if context_col is not None:
            context_opts = self.bar._primary_value_options(sd, context_col, include_total=True)
            context_value = next((o["value"] for o in context_opts if o["value"] == "Total"), context_opts[0]["value"])

        fig_count, note_count = self.bar.update_chart(
            dataset_name, x_col, context_value, "distribution", None, dist_var, category, "count"
        )
        fig_percent, note_percent = self.bar.update_chart(
            dataset_name, x_col, context_value, "distribution", None, dist_var, category, "percent"
        )

        self.assertTrue(fig_count.data, "Count chart should contain at least one bar trace.")
        self.assertTrue(fig_percent.data, "Percent chart should contain at least one bar trace.")
        self.assertIn("Count", fig_count.layout.title.text)
        self.assertIn("Percent", fig_percent.layout.title.text)
        self.assertIn("Distribution Category mode", note_count)
        self.assertIn("Distribution Category mode", note_percent)

    def test_scalar_mode_includes_distribution_to_scalar(self):
        # SEN dataset has a quantitative distribution ("age"), so mean/median should be present.
        sd = None
        for name, candidate in self.bar._datasets.items():
            if "sen" in name.lower():
                sd = candidate
                break
        if sd is None:
            self.skipTest("SEN dataset not loaded; cannot verify distribution to-scalar options.")

        scalar_opts = self.bar._numeric_secondary_options(sd)
        values = [o["value"] for o in scalar_opts]
        self.assertTrue(any(v.endswith("_mean") for v in values), "Expected at least one to-scalar mean option.")
        self.assertTrue(any(v.endswith("_median") for v in values), "Expected at least one to-scalar median option.")


if __name__ == "__main__":
    unittest.main()

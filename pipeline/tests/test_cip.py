import unittest

import pandas as pd
from pandas.testing import assert_frame_equal
from pipeline.cip_crosswalk import CIPCodeCrosswalk, DHSClassifier, NCSESClassifier

from scipeds.data.enums import (
    FieldTaxonomy,
    NCSESDetailedFieldGroup,
    NCSESFieldGroup,
    NCSESSciGroup,
    NSFBroadField,
)


class CrosswalkTests(unittest.TestCase):
    def setUp(self):
        self.cw = CIPCodeCrosswalk()

    def test_2010(self):
        old_code = "22.0303"
        old_title = "Court Reporting/Court Reporter"
        new_code = "22.0303"
        new_title = "Court Reporting and Captioning/Court Reporter"
        new_codes = self.cw.convert_to_cip2020(2010, codes=old_code, titles=old_title)
        assert new_code == new_codes.cip2020.iloc[0]
        assert new_title == new_codes.cip2020_title.iloc[0]

        new_codes = self.cw.convert_to_cip2020(2019, codes=old_code, titles=old_title)
        assert new_code == new_codes.cip2020.iloc[0]
        assert new_title == new_codes.cip2020_title.iloc[0]

    def test_2000(self):
        old_code = "26.0405"
        old_title = "Neuroanatomy"
        new_code = "26.1502"
        new_title = "Neuroanatomy"
        new_codes = self.cw.convert_to_cip2020(2000, codes=old_code, titles=old_title)
        assert new_code == new_codes.cip2020.iloc[0]
        assert new_title == new_codes.cip2020_title.iloc[0]

        new_codes = self.cw.convert_to_cip2020(2009, codes=old_code, titles=old_title)
        assert new_code == new_codes.cip2020.iloc[0]
        assert new_title == new_codes.cip2020_title.iloc[0]

    def test_1990(self):
        old_code = "52.0405"
        old_title = "Court Reporter"
        new_code = "22.0303"
        new_title = "Court Reporting and Captioning/Court Reporter"
        new_codes = self.cw.convert_to_cip2020(1990, codes=old_code, titles=old_title)
        assert new_code == new_codes.cip2020.iloc[0]
        assert new_title == new_codes.cip2020_title.iloc[0]

        new_codes = self.cw.convert_to_cip2020(1999, codes=old_code, titles=old_title)
        assert new_code == new_codes.cip2020.iloc[0]
        assert new_title == new_codes.cip2020_title.iloc[0]

    def test_1985(self):
        old_code = "07.0103"
        old_title = "Bookkeeping"
        new_code = "52.0302"
        new_title = "Accounting Technology/Technician and Bookkeeping"
        new_codes = self.cw.convert_to_cip2020(1984, codes=old_code, titles=old_title)
        assert new_code == new_codes.cip2020.iloc[0]
        assert new_title == new_codes.cip2020_title.iloc[0]

        new_codes = self.cw.convert_to_cip2020(1989, codes=old_code, titles=old_title)
        assert new_code == new_codes.cip2020.iloc[0]
        assert new_title == new_codes.cip2020_title.iloc[0]

    def test_1991_exception(self):
        # Some CIP codes are present in 1991 data, but not present in the
        # 1990 -> 2k CIP crosswalk (because they needs to be converted via
        # the 1985 -> 1990 crosswalk first).
        # This tests that 1991 data is indeed going through the 85 -> 90
        # CIP mapper
        old_code = "06.0401"
        old_title = "Business Administration"
        new_code = "52.0201"
        new_title = "Business Administration and Management, General"
        new_codes = self.cw.convert_to_cip2020(1991, codes=old_code, titles=old_title)
        assert new_code == new_codes.cip2020.iloc[0]
        assert new_title == new_codes.cip2020_title.iloc[0]

    def test_1990_pdf_mapping(self):
        # Some CIP codes are only in the pdf, make sure those are mapped correctly
        old_code = "07.0305"
        old_title = "Business Data Programming"  # from the pdf file
        new_code = "11.0202"
        new_title = "Computer Programming, Specific Applications"
        new_codes = self.cw.convert_to_cip2020(1990, codes=old_code, titles=old_title)
        assert new_code == new_codes.cip2020.iloc[0]
        assert new_title == new_codes.cip2020_title.iloc[0]

        # Some codes are in the PDF twice; we want to take the first instance
        old_code = "17.0499"
        old_title = "Mental Health/Human Services, Other"
        new_code = "51.1504"  # maps to 51.0301, not 51.1599, which gets remapped further
        new_title = "Community Health Services/Liaison/Counseling"
        new_codes = self.cw.convert_to_cip2020(1990, codes=old_code, titles=old_title)
        assert new_code == new_codes.cip2020.iloc[0]
        assert new_title == new_codes.cip2020_title.iloc[0]

    def test_1990_pdf_parsing(self):
        cw = self.cw.crosswalk[(1984, 1991)]["cip_map"]
        assert set([len(s) for s in cw.keys()]) == {7}
        assert set([len(s) for s in cw.values()]) == {7}

    def test_missing(self):
        # Missing codes (and titles) should not change
        old_code = "91.9129"
        old_title = "Made up title"
        new_codes = self.cw.convert_to_cip2020(1990, codes=old_code, titles=old_title)
        assert new_codes.cip2020.iloc[0] == old_code
        assert new_codes.cip2020_title.iloc[0] == old_title


class NCSESTests(unittest.TestCase):
    def setUp(self):
        self.nc = NCSESClassifier()

    def test_inputs(self):
        expected = pd.DataFrame.from_dict(
            {
                "index": ["01.0308"],
                "columns": [
                    "cip_title",
                    FieldTaxonomy.ncses_sci_group.value,
                    FieldTaxonomy.ncses_field_group.value,
                    FieldTaxonomy.ncses_detailed_field_group.value,
                    FieldTaxonomy.nsf_broad_field.value,
                ],
                "data": [
                    [
                        "Agroecology and Sustainable Agriculture",
                        NCSESSciGroup.sci.value,
                        NCSESFieldGroup.sci_life_sci.value,
                        NCSESDetailedFieldGroup.ag_sci.value,
                        NSFBroadField.ag_and_bio_sci.value,
                    ]
                ],
                "index_names": ["cipcode"],
                "column_names": [None],
            },
            orient="tight",
        )

        # Input is one string
        codes = "01.0308"
        result = self.nc.classify(codes)
        assert_frame_equal(result, expected)

        # Input is a list with one string
        codes = ["01.0308"]
        result = self.nc.classify(codes)
        assert_frame_equal(result, expected)

        codes = pd.Series(data=["01.0308"])
        result = self.nc.classify(codes)
        assert_frame_equal(result, expected)

    def test_known_inputs(self):
        codes = ["14.0202", "04.0200", "98.7654", "51.0507"]
        expected = pd.DataFrame.from_dict(
            {
                "index": codes,
                "columns": [
                    "cip_title",
                    FieldTaxonomy.ncses_sci_group.value,
                    FieldTaxonomy.ncses_field_group.value,
                    FieldTaxonomy.ncses_detailed_field_group.value,
                    FieldTaxonomy.nsf_broad_field.value,
                ],
                "data": [
                    [
                        "Astronautical Engineering",
                        NCSESSciGroup.sci.value,
                        NCSESFieldGroup.engineering.value,
                        NCSESDetailedFieldGroup.aerospace_eng.value,
                        NSFBroadField.eng.value,
                    ],
                    [
                        "Pre-Architecture Studies",
                        NCSESSciGroup.non_sci.value,
                        NCSESFieldGroup.arch_dsgn.value,
                        NCSESDetailedFieldGroup.arch_dsgn.value,
                        NSFBroadField.non_stem.value,
                    ],
                    [
                        "Unknown",
                        NCSESSciGroup.unknown.value,
                        NCSESFieldGroup.unknown.value,
                        NCSESDetailedFieldGroup.unknown.value,
                        NSFBroadField.non_stem.value,
                    ],
                    [
                        "Oral/Maxillofacial Surgery",
                        NCSESSciGroup.non_sci.value,
                        NCSESFieldGroup.nonsci_life_sci.value,
                        NCSESDetailedFieldGroup.medical_sci.value,
                        NSFBroadField.non_stem.value,
                    ],
                ],
                "index_names": ["cipcode"],
                "column_names": [None],
            },
            orient="tight",
        )

        result = self.nc.classify(codes)
        assert_frame_equal(result, expected)

    def test_titles(self):
        codes = ["51.3205", "14.0202", "04.0200", "98.7654", "51.0507"]
        titles = [
            "History of Medicine",
            "Astronautical Engineering",
            "Pre-Architecture Studies",
            "Unknown",
            "Oral/Maxillofacial Surgery",
        ]
        translated = self.nc.get_titles(codes)
        assert translated.values.tolist() == titles


class DHSTests(unittest.TestCase):
    def setUp(self):
        self.dc = DHSClassifier()

    def test_inputs(self):
        expected = pd.DataFrame.from_dict(
            {
                "index": ["01.0308"],
                "columns": ["dhs_stem"],
                "data": [[True]],
                "index_names": ["cipcode"],
                "column_names": [None],
            },
            orient="tight",
        )

        # Input is one string
        codes = "01.0308"
        result = self.dc.classify(codes)
        assert_frame_equal(result, expected)

        # Input is a list with one string
        codes = ["01.0308"]
        result = self.dc.classify(codes)
        assert_frame_equal(result, expected)

        # Input is pd.Index
        codes = pd.Index(["01.0308"])
        result = self.dc.classify(codes)
        assert_frame_equal(result, expected)

        codes = pd.Series(data=["01.0308"])
        result = self.dc.classify(codes)
        assert_frame_equal(result, expected)

    def test_known_nonstem(self):
        codes = ["43.0110", "39.0302"]
        result = self.dc.classify(codes)
        assert not result.dhs_stem.any()

    def test_known_stem(self):
        codes = [
            "01.0308",
            "01.1002",
            "01.1099",
            "10.0304",
            "11.0101",
            "14.0100",
            "14.0101",
            "14.1004",
            "14.1099",
            "14.3500",
            "14.3501",
            "15.0305",
            "15.0306",
            "15.0903",
            "15.0999",
            "26.0208",
            "26.0209",
            "26.0802",
            "26.0803",
            "26.1200",
            "26.1201",
            "27.0304",
            "27.0305",
            "29.0405",
            "29.0406",
            "40.0100",
            "40.0101",
            "40.0801",
            "40.0802",
            "42.2799",
            "42.2804",
            "52.1399",
        ]
        results = self.dc.classify(codes)
        assert results.dhs_stem.all()

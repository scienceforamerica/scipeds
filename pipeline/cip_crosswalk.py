from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Union

import fitz  # type: ignore
import pandas as pd

import pipeline.settings
from scipeds.data.enums import (
    NSF_REPORT_BROAD_FIELD_MAP,
    FieldTaxonomy,
    NCSESDetailedFieldGroup,
    NCSESFieldGroup,
    NCSESSciGroup,
    NSFBroadField,
)
from scipeds.utils import convert_to_series

PIPELINE_ASSETS = Path(__file__).resolve().parents[0] / "assets"


class CIPCodeCrosswalk:
    """Handles cross-walking of CIP codes from the past to 2020 CIP codes"""

    # Correct mapping for Physiology, which only mapped to 26.09
    CORRECTIONS_1990 = {"51.1313": "26.0901"}

    def __init__(
        self,
        crosswalk_dir: Path = pipeline.settings.RAW_DATA_DIR
        / pipeline.settings.CROSSWALKS_DIRNAME,
    ):
        # Load in/standardize all crosswalk CSVs
        self.crosswalk_dir = crosswalk_dir
        self.crosswalk: Dict[Tuple[int, int], dict] = {}
        self._load_2010_to_2020_crosswalk()
        self._load_2000_to_2010_crosswalk()
        self._load_1990_to_2000_crosswalk()
        self._load_1985_to_1990_crosswalk()

        self.year_ranges = sorted(list(self.crosswalk.keys()), key=lambda x: x[0])
        self.min_year = min(yr[0] for yr in self.year_ranges)

    def _parse_1990_cip_pdf(self):
        pdf_path = PIPELINE_ASSETS / "91396_subset.pdf"
        doc = fitz.open(pdf_path)

        # Extract text from all pages
        pdf_text = "\n".join([page.get_text("text") for page in doc])
        pdf_text = pdf_text.split("\n")

        # Remove column headers
        pdf_text = pdf_text[16:]

        # Remove the Chapter headers
        ch_idx_to_remove = {
            "CHAPTER TWO": [-1, 1],  # Ch. 2 has an empty string before the CHAPTER TWO line
            "CHAPTER THREE": [0, 1],  # But Ch. 3 does not
            "CHAPTER FOUR": [0, 1],
            "CHAPTER FIVE": [-1, 1],
            "CHAPTER SIX": [-1, 1],
        }
        rm_idxs = []
        for k, v in ch_idx_to_remove.items():
            i = pdf_text.index(k)
            rm_idxs += list(range(i + v[0], i + v[1] + 1))
        pdf_text = [t for i, t in enumerate(pdf_text) if i not in rm_idxs]

        # Remove deleted CIP codes, they don't have a 90 CIP so there's only two elements for these
        deleted_idxs = [i for i, t in enumerate(pdf_text) if t == "Deleted"]
        rm_idxs = []
        for i in deleted_idxs:
            rm_idxs += [i - 1, i]
        pdf_text = [t for i, t in enumerate(pdf_text) if i not in rm_idxs]

        # Remove one "Assign to Specific Hobby (see Appendix D)", also doesn't have a 90 CIP
        deleted_idxs = [
            i for i, t in enumerate(pdf_text) if t == "Assign to Specific Hobby (see Appendix D)"
        ]
        rm_idxs = []
        for i in deleted_idxs:
            rm_idxs += [i - 1, i]
        pdf_text = [t for i, t in enumerate(pdf_text) if i not in rm_idxs]

        # Remove any remaining empty strings
        pdf_text = [t for t in pdf_text if t != ""]

        # For some reason only this one CIP Title got parsed incorrectly
        idx = 1004
        pdf_text = pdf_text[:idx] + [" ".join(pdf_text[idx : idx + 8])] + pdf_text[idx + 8 :]

        pdf_df = (
            pd.DataFrame(
                data=[pdf_text[::3], pdf_text[1::3], pdf_text[2::3]],
                index=["CIP85", "CIP90", "CIPTITLE90"],
            )
            .T.replace("", None)
            .dropna(how="all")
        )

        return pdf_df

    def _load_2010_to_2020_crosswalk(self):
        """Load and clean/process the CIP2010 -> CIP2020 crosswalk"""
        year_range = (2010, 2019)
        file = self.crosswalk_dir / "2010-2019" / "Crosswalk2010to2020.csv"
        df = pd.read_csv(file, dtype=str)
        origin_col = "CIPCode2010"
        destination_col = "CIPCode2020"
        destination_title_col = "CIPTitle2020"
        action_col = "Action"

        for col in [origin_col, destination_col]:
            df[col] = df[col].str.strip('="')

        moved_cip_df = df[df[action_col].str.lower() == "moved to"]
        cip_map = dict(zip(moved_cip_df[origin_col], moved_cip_df[destination_col]))

        cip_titles_df = df[~df[destination_title_col].str.contains("Deleted")].copy()

        # CipTitles in the 2010 -> 2020 crosswalk have periods that aren't in the raw data
        cip_titles_df[destination_title_col] = cip_titles_df[destination_title_col].str.strip(".")
        title_map = dict(zip(cip_titles_df[destination_col], cip_titles_df[destination_title_col]))

        mappers = {"cip_map": cip_map, "title_map": title_map}
        self.crosswalk[year_range] = mappers

    def _load_2000_to_2010_crosswalk(self):
        """Load and clean/process the CIP2000 -> CIP2010 crosswalk"""
        year_range = (2000, 2009)
        file = self.crosswalk_dir / "2000-2009" / "Crosswalk2000to2010.csv"
        df = pd.read_csv(file, dtype=str)
        origin_col = "CIPCode2000"
        destination_col = "CIPCode2010"
        destination_title_col = "CIPTitle2010"
        action_col = "Action"

        for col in [origin_col, destination_col]:
            df[col] = df[col].str.strip('="')

        moved_cip_df = df[df[action_col].str.lower() == "moved to"]
        cip_map = dict(zip(moved_cip_df[origin_col], moved_cip_df[destination_col]))

        cip_titles_df = df[~df[destination_title_col].str.contains("Deleted")].copy()

        # CipTitles in the 2000 -> 2010 crosswalk have periods that aren't in the raw data
        cip_titles_df[destination_title_col] = cip_titles_df[destination_title_col].str.strip(".")
        title_map = dict(zip(cip_titles_df[destination_col], cip_titles_df[destination_title_col]))

        mappers = {"cip_map": cip_map, "title_map": title_map}
        self.crosswalk[year_range] = mappers

    def _load_1990_to_2000_crosswalk(self):
        """Load and clean/process the CIP1990 -> CIP2000 crosswalk"""
        year_range = (1990, 1999)
        file = self.crosswalk_dir / "1985-1999" / "CIP.XLS"
        sheet_name = "Crosswalk_CIP90toCIP2K"
        df = pd.read_excel(file, sheet_name=sheet_name, dtype=str)

        origin_col = "CIPCODE90"
        destination_col = "CIPCODE2k"
        destination_title_col = "CIPTEXT2K"

        # Get rid of any rows without an original CIP code (new CIP codes),
        # without a new CIP code (deleted), and indicators for moved/deleted
        # codes ("report as" and "deleted" in the CIP code column)
        df = df[
            (df[origin_col].notna())
            & (df[destination_col].notna())
            & ~(df[destination_title_col].str.contains("Deleted"))
            & ~(df[destination_col].fillna("").str.contains("Report"))
            & ~(df[destination_col].fillna("").str.contains("Deleted"))
        ]

        df = df[df[origin_col] != df[destination_col]]
        cip_map = dict(zip(df[origin_col], df[destination_col]))
        cip_code_to_title_map = dict(zip(df[destination_col], df[destination_title_col]))

        cip_map.update(self.CORRECTIONS_1990)
        mappers = {"cip_map": cip_map, "title_map": cip_code_to_title_map}

        self.crosswalk[year_range] = mappers

    def _load_1985_to_1990_crosswalk(self):
        """Load and clean/process the CIP1990 -> CIP2000 crosswalk"""
        year_range = (
            1984,
            1991,
        )  # 1990 and 1991 have some CIP codes that need to get processed through this crosswalk
        file = self.crosswalk_dir / "1985-1999" / "CIP.XLS"
        sheet_name = "Crosswalk_CIP85toCIP90"
        df = pd.read_excel(file, sheet_name=sheet_name, dtype=str)

        origin_col = "CIP85"
        destination_col = "CIP90"
        destination_title_col = "CIPTITLE90"

        # Get rid of any rows without an original CIP code (new CIP codes),
        # without a new CIP code (deleted), and indicators for moved/deleted
        # codes ("report as" and "deleted" in the CIP code column)
        df = df[
            (df[origin_col].notna())
            & (df[destination_col].notna())
            & ~(df[destination_title_col].str.contains("Deleted"))
            & ~(df[destination_col].fillna("").str.contains("Report"))
            & ~(df[destination_col].fillna("").str.contains("Deleted"))
        ]

        df = df[df[origin_col] != df[destination_col]]
        cip_map = dict(zip(df[origin_col], df[destination_col]))
        cip_code_to_title_map = dict(zip(df[destination_col], df[destination_title_col]))

        # Add in the CIP mappings from the pdf
        df = self._parse_1990_cip_pdf()
        origin_col = "CIP85"
        destination_col = "CIP90"
        destination_title_col = "CIPTITLE90"

        cip_map = {
            **dict(zip(df[origin_col], df[destination_col])),
            **cip_map,
        }  # Use the Excel in cases where a CIP is in both
        cip_code_to_title_map = {
            **dict(zip(df[destination_col], df[destination_title_col])),
            **cip_code_to_title_map,
        }

        mappers = {"cip_map": cip_map, "title_map": cip_code_to_title_map}

        self.crosswalk[year_range] = mappers

    def walk(
        self, year_range: Tuple[int, int], codes: pd.Series, titles: Optional[pd.Series] = None
    ) -> Tuple[pd.Series, pd.Series]:
        """Map from old set of codes (in old year range) to newer set of codes"""
        mappers = self.crosswalk[year_range]
        cip_map, title_map = mappers["cip_map"], mappers["title_map"]
        new_codes: pd.Series = codes.map(cip_map).fillna(codes)
        if titles is not None:
            new_titles = titles.copy()
            new_titles = codes.map(title_map).fillna(titles)
        else:
            new_titles = new_codes.map(title_map)
        return new_codes, new_titles

    def convert_to_cip2020(
        self,
        year: int,
        codes: Union[str, List[str], pd.Series],
        titles: Optional[Union[str, List[str], pd.Series]] = None,
    ) -> pd.DataFrame:
        """Convert from old CIP codes to CIP 2020 CIP Codes and Titles"""
        # Check that year is within range
        if year < self.min_year:
            raise ValueError(f"Year provided must be at least {self.min_year} got {year}.")

        # Convert parameters for consistency
        new_codes = convert_to_series(codes)
        if titles is not None:
            new_titles = convert_to_series(titles)
        else:
            new_titles = None

        # Iteratively walk forward in time
        for yr in self.year_ranges:
            if year <= yr[-1]:
                new_codes, new_titles = self.walk(yr, new_codes, new_titles)

        # Combine into one dataframe
        if new_titles is not None:
            df = pd.concat([new_codes, new_titles], axis=1)
            df.columns = pd.Index(["cip2020", "cip2020_title"])
        else:
            new_codes.name = "cip2020"
            df = new_codes.to_frame()
        return df


class NCSESClassifier:
    """Class for converting CIP codes to NCSES hierarchical classification"""

    # Some codes are missing from NCSES, fill with our best guess
    MISSING_CODES = {
        "03.0405": {
            "ncses_cip_string": "Logging/Timber Harvesting",
            FieldTaxonomy.ncses_sci_group: NCSESSciGroup.sci.value,
            FieldTaxonomy.ncses_field_group: NCSESFieldGroup.sci_life_sci.value,
            FieldTaxonomy.ncses_detailed_field_group: NCSESDetailedFieldGroup.ag_sci.value,
        },
        "15.1799": {
            "ncses_cip_string": "Energy Systems Technologies/Technicials, Other",
            FieldTaxonomy.ncses_sci_group: NCSESSciGroup.non_sci.value,
            FieldTaxonomy.ncses_field_group: NCSESFieldGroup.sci_eng_technologies.value,
            FieldTaxonomy.ncses_detailed_field_group: (
                NCSESDetailedFieldGroup.engineering_technologies.value
            ),
        },
        "16.1402": {
            "ncses_cip_string": "Indonesian/Malay Languages and Literatures",
            FieldTaxonomy.ncses_sci_group: NCSESSciGroup.non_sci.value,
            FieldTaxonomy.ncses_field_group: NCSESFieldGroup.humanities.value,
            FieldTaxonomy.ncses_detailed_field_group: NCSESDetailedFieldGroup.languages.value,
        },
        "51.2404": {
            "ncses_cip_string": "Veterinary Medicine (DVM)",
            FieldTaxonomy.ncses_sci_group: NCSESSciGroup.non_sci.value,
            FieldTaxonomy.ncses_field_group: NCSESFieldGroup.nonsci_life_sci.value,
            FieldTaxonomy.ncses_detailed_field_group: NCSESDetailedFieldGroup.medical_sci.value,
        },
        "51.3205": {
            "ncses_cip_string": "History of Medicine",
            FieldTaxonomy.ncses_sci_group: NCSESSciGroup.sci.value,
            FieldTaxonomy.ncses_field_group: NCSESFieldGroup.social_sciences.value,
            FieldTaxonomy.ncses_detailed_field_group: (
                NCSESDetailedFieldGroup.social_sci_other.value
            ),
        },
        "20.0102": {
            "ncses_cip_string": "Child Development, Care & Guidance",
            FieldTaxonomy.ncses_sci_group: NCSESSciGroup.non_sci.value,
            FieldTaxonomy.ncses_field_group: NCSESFieldGroup.home_ec.value,
            FieldTaxonomy.ncses_detailed_field_group: (NCSESDetailedFieldGroup.home_ec.value),
        },
        "07.0608": {
            "ncses_cip_string": "Word Processing",
            FieldTaxonomy.ncses_sci_group: NCSESSciGroup.non_sci.value,
            FieldTaxonomy.ncses_field_group: NCSESFieldGroup.business_and_mgmt.value,
            FieldTaxonomy.ncses_detailed_field_group: (
                NCSESDetailedFieldGroup.business_and_mgmt.value
            ),
        },
    }

    def __init__(self, filepath: Path = PIPELINE_ASSETS / "ncses_stem_classification_table.csv"):
        """Read the NCSES file into an internal df to use for classification"""
        df = pd.read_csv(
            filepath,
            skiprows=[0, 1, 2, 3, 4, 5, 7, 8, 9],
            skipfooter=4,
            header=0,
            usecols=[0, 1, 2, 3],
            quotechar='"',
            dtype=str,
            engine="python",
        )

        # Rename columns
        df.columns = pd.Index(
            [
                "ncses_sci_group",
                "ncses_field_group",
                "ncses_detailed_field_group",
                "ncses_cip_string",
            ]
        )

        # Remove totals
        df = df[~df.ncses_cip_string.str.startswith("Total for selected values")]

        # Replace non-sci "Life Sciences" field group with differentiated field group
        mask = (df.ncses_sci_group == NCSESSciGroup.non_sci.value) & (
            df.ncses_field_group == NCSESFieldGroup.sci_life_sci.value
        )
        df.loc[mask, "ncses_field_group"] = NCSESFieldGroup.nonsci_life_sci.value

        # Split CIP codes
        df["cipcode"] = df["ncses_cip_string"].apply(lambda x: x.split(" - ")[0].strip())
        df["ncses_cip_string"] = df["ncses_cip_string"].apply(
            lambda x: x.split(" - ")[1].strip(" .")
        )
        df.set_index("cipcode", inplace=True)

        # Store internal representation
        self.df = df.copy()

        # Create mappings
        self.sg_map = df["ncses_sci_group"].to_dict()
        self.fg_map = df["ncses_field_group"].to_dict()
        self.dfg_map = df["ncses_detailed_field_group"].to_dict()
        self.title_map = df["ncses_cip_string"].to_dict()

        # Update mappings
        for code, values in self.MISSING_CODES.items():
            self.title_map.update({code: values["ncses_cip_string"]})
            self.sg_map.update({code: values[FieldTaxonomy.ncses_sci_group]})
            self.fg_map.update({code: values[FieldTaxonomy.ncses_field_group]})
            self.dfg_map.update({code: values[FieldTaxonomy.ncses_detailed_field_group]})

    def get_titles(self, codes: Union[str, Iterable[str]]) -> pd.Series:
        """Return NCSES title strings corresponding to 2020 CIP Codes

        Args:
            codes (Union[str, List[str], pd.Series]): CIP 2020 codes

        Returns:
            pd.Series: NCSES title strings
        """
        codes = convert_to_series(codes).rename("cip_title").astype(str)
        return codes.map(self.title_map).fillna("Unknown")

    def classify(
        self,
        original_codes: Union[str, List[str], pd.Series],
        codes_2020: str | List[str] | pd.Series | None = None,
    ) -> pd.DataFrame:
        """Classify CIP code(s) in the NCSES classification.

        In all cases, prefer the classification of the original CIP code, but use the 2020 version
        if the original is unclassified.

        Args:
            original_codes (Union[str, List[str], pd.Series]): CIP code(s)
            codes_2020 (Union[str, List[str], pd.Series]): CIP 2020 code(s) to classify, optional.
                Default: None

        Returns:
            pd.DataFrame: Data frame indexed by CIP code with each level of NCSES
                classifcation as columns
        """
        # Classify the original cip codes
        original_codes = convert_to_series(original_codes)
        titles = self.get_titles(original_codes)
        sgs = original_codes.map(self.sg_map)
        fgs = original_codes.map(self.fg_map)
        dfgs = original_codes.map(self.dfg_map)
        # map nsf broad fields at ncses field group level and sci group level
        nsf_fgs = fgs.map(NSF_REPORT_BROAD_FIELD_MAP)
        nsf_sgs = sgs.map(NSF_REPORT_BROAD_FIELD_MAP)
        nsfs = nsf_fgs.combine_first(nsf_sgs)

        # Fill any null values with the 2020 definitions or "unknown"
        if codes_2020 is not None:
            codes_2020 = convert_to_series(codes_2020)
            sgs_2020 = codes_2020.map(self.sg_map)
            fgs_2020 = codes_2020.map(self.fg_map)
            dfgs_2020 = codes_2020.map(self.dfg_map)
            nsf_fgs_2020 = fgs_2020.map(NSF_REPORT_BROAD_FIELD_MAP)
            nsf_sgs_2020 = sgs_2020.map(NSF_REPORT_BROAD_FIELD_MAP)
            nsfs_2020 = nsf_fgs_2020.combine_first(nsf_sgs_2020)

            # Combine the two classifications
            sgs_2020.index = sgs.index
            fgs_2020.index = fgs.index
            dfgs_2020.index = dfgs.index
            nsfs_2020.index = nsfs.index
            sgs = sgs.combine_first(sgs_2020)
            fgs = fgs.combine_first(fgs_2020)
            dfgs = dfgs.combine_first(dfgs_2020)
            nsfs = nsfs.combine_first(nsfs_2020)

        # Fill any remaining null values with "unknown"
        sgs = sgs.fillna(NCSESSciGroup.unknown.value)
        fgs = fgs.fillna(NCSESFieldGroup.unknown.value)
        dfgs = dfgs.fillna(NCSESDetailedFieldGroup.unknown.value)
        nsfs = nsfs.fillna(NSFBroadField.unknown.value)

        df = pd.concat([original_codes, titles, sgs, fgs, dfgs, nsfs], axis=1)
        df.columns = pd.Index(
            [
                "cipcode",
                "cip_title",
                "ncses_sci_group",
                "ncses_field_group",
                "ncses_detailed_field_group",
                "nsf_broad_field",
            ]
        )
        df.set_index("cipcode", inplace=True)
        return df


class DHSClassifier:
    def __init__(self, filepath: Path = PIPELINE_ASSETS / "dhs_stem_classification_table.csv"):
        """Read the NCSES file into an internal df to use for classification"""
        df = pd.read_csv(
            filepath,
            header=0,
            dtype=str,
        )
        df.columns = pd.Index(
            [
                "cip_2digit",
                "cipcode",
                "ciptitle",
            ]
        )
        df.set_index("cipcode", inplace=True)

        self.df = df

    def classify(self, codes: Union[str, List[str], pd.Series, pd.Index]) -> pd.DataFrame:
        """Classify a set of CIP codes as belonging (True) or not belonging (False) to the DHS
        set of STEM CIP codes

        Args:
            codes (Union[str, List[str], pd.Series]): CIP code(s)

        Returns:
            pd.DataFrame: DataFrame indexed by input codes with one bool column indicating DHS STEM
        """
        codes = convert_to_series(codes)
        is_dhs_stem = codes.isin(self.df.index)
        df = pd.concat([codes, is_dhs_stem], axis=1)
        df.columns = pd.Index(["cipcode", "dhs_stem"])
        df.set_index("cipcode", inplace=True)
        return df

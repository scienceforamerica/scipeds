import unittest
from itertools import chain

from scipeds.data.enums import (
    NCSES_HIERARCHY,
    NCSESDetailedFieldGroup,
    NCSESFieldGroup,
    NCSESSciGroup,
)


class TestFields(unittest.TestCase):
    def test_sci_group(self):
        self.assertCountEqual(list(NCSES_HIERARCHY.keys()), list(NCSESSciGroup))

    def test_field_group(self):
        fields = list(
            chain.from_iterable(list(NCSES_HIERARCHY[sg].keys()) for sg in NCSESSciGroup)
        )
        self.assertCountEqual(fields, list(NCSESFieldGroup))

    def test_detailed_field_group(self):
        detailed_fields = list(
            chain.from_iterable(
                list(
                    chain.from_iterable(list(NCSES_HIERARCHY[sg].values())) for sg in NCSESSciGroup
                )
            )
        )
        self.assertCountEqual(detailed_fields, list(NCSESDetailedFieldGroup))

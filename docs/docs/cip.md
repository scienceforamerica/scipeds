# `pipeline.cip_crosswalk`

The code in `pipeline.cip_crosswalk` handles transformations related to CIP (Classification of Instructional Program) codes:

- The `CIPCodeCrosswalk` handles transforming old CIP codes (CIP90, CIP2k, and CIP2010) to the current standard (CIP2020)
- The `DHSClassifier` classifies CIP codes according to whether or not the appear in the Department of Homeland Security's [STEM Designated Degree Program List](https://www.ice.gov/sites/default/files/documents/stem-list.pdf)
- The `NCSESClassifier` classifies CIP codes according to the [NCSES Alternate Classification](https://ncsesdata.nsf.gov/builder/ipeds_c) and the broad fields included in the NSF's [Diversity and STEM reports](https://ncses.nsf.gov/pubs/nsf23315/)

::: pipeline.cip_crosswalk
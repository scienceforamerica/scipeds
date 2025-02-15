from enum import Enum
from enum import property as enum_property


class Gender(str, Enum):
    """Enumeration for gender.

    Note that [IPEDS reports gender as binary](
    https://surveys.nces.ed.gov/ipeds/public/survey-materials/faq?faqid=11).

    Attributes:
        men (str): men
        women (str): women
    """

    men = "men"
    women = "women"


class RaceEthn(str, Enum):
    """Enumeration for race/ethnicity

    Note that the categories "Two or more races" and "Native Hawaiian or Other Pacific Islander"
    were added for enrolllment data beginning in 2010-2011 and in completions data beginning in
    2011-2012. `scipeds` will warn users if their selected `QueryFilters` include years
    with mixed categories.

    See [IPEDS](https://nces.ed.gov/ipeds/report-your-data/race-ethnicity-reporting-changes)
    for more details.

    Attributes:
        american_indian (str): American Indian or Alaska Native
        asian (str): Asian
        black_or_aa (str): Black or African American
        hispanic (str): Hispanic or Latino
        hawaiian_pi (str): Native Hawaiian or Other Pacific Islander
        nonres (str): Non-resident alien
        two_or_more (str): Two or more races
        white (str): White
        unknown (str): Unknown
    """

    american_indian = "American Indian or Alaska Native"
    asian = "Asian"
    black_or_aa = "Black or African American"
    hispanic = "Hispanic or Latino"
    hawaiian_pi = "Native Hawaiian or Other Pacific Islander"
    nonres = "Non-resident alien"
    two_or_more = "Two or more races"
    white = "White"
    unknown = "Unknown"


class Grouping(str, Enum):
    gender = "gender"
    race_ethnicity = "race_ethnicity"
    intersectional = "intersectional"

    @enum_property
    def label_suffix(self) -> str:
        return f"within_{self.value}" if self is not self.intersectional else self.value

    @enum_property
    def students_suffix(self) -> str:
        return "students" if self is self.intersectional else ""

    @enum_property
    def grouping_columns(self) -> list[str]:
        return (
            [self.value]
            if self is not self.intersectional
            else [self.race_ethnicity.value, self.gender.value]  # type: ignore
        )


class AwardLevel(str, Enum):
    """Enumeration for award level

    Note that in 2011, award level codes changed. The `scipeds` pipeline attempts
    to transform old codes in accordance with [IPEDS guidance](
    https://nces.ed.gov/ipeds/report-your-data/data-tip-sheet-reporting-graduate-awards).

    Attributes:
        lt_12w (str): Certificates of less than 12 weeks
        gt_12w_lt_1y (str): Certificates of at least 12 weeks but less than 1 year
        lt1 (str): Award of less than 1 academic year
        gt1_lt2 (str): Award of at least 1 but less than 2 academic years
        associates (str): Associate's degree
        gt2_lt4 (str): Award of at least 2 but less than 4 academic years
        bachelors (str): Bachelor's degree
        postbac (str): Postbaccalaureate certificate
        masters (str): Master's degree
        postmas (str): Post-master's certificates
        doctor_research (str): Doctor's degree - research/scholarship
        doctor_professional (str): Doctor's degree - professional practice
        doctor_other (str): Doctor's degree - professional practice
        unknown (str): Unknown
    """

    lt_12w = "Certificates of less than 12 weeks"
    gt_12w_lt_1y = "Certificates of at least 12 weeks but less than 1 year"
    lt1 = "Award of less than 1 academic year"
    gt1_lt2 = "Award of at least 1 but less than 2 academic years"
    associates = "Associate's degree"
    gt2_lt4 = "Award of at least 2 but less than 4 academic years"
    bachelors = "Bachelor's degree"
    postbac = "Postbaccalaureate certificate"
    masters = "Master's degree"
    postmas = "Post-master's certificates"
    doctor_research = "Doctor's degree - research/scholarship"
    doctor_professional = "Doctor's degree - professional practice"
    doctor_other = "Doctor's degree - professional practice"
    unknown = "Unknown"


class InstitutionProperty(str, Enum):
    tech_school = "Tech school"
    health_school = "Health school"
    carnegie_classification_2021_basic = "Carnegie classification"
    geographic_region = "Geographic region"
    historically_black_college_or_university = "HBCU"


class FieldTaxonomy(str, Enum):
    """Enumeration for field taxonomy.

    The values in this enumeration also correspond to columns in the pre-processed database

    Attributes:
        cip (str): The 2020 CIP code
        original_cip (str): The originally recorded CIP code
        ncses_sci_group (str): The NCSES [Science & Engineering Alternative Classification](
            https://ncsesdata.nsf.gov/sere/2018/html/sere18-dt-taba001.html)
        ncses_field_group (str): The NCSES [Broad Field Alternative Classification](
            https://ncsesdata.nsf.gov/sere/2018/html/sere18-dt-taba001.html)
        ncses_detailed_field_group (str): The NCSES
            [Detailed Field Alternative Classification](
            https://ncsesdata.nsf.gov/sere/2018/html/sere18-dt-taba001.html)
        nsf_broad_field (str): The NSF Broad Field Classification in the
            [NSF Diversity in STEM report](
            https://ncses.nsf.gov/pubs/nsf23315/report)
        dhs_stem (str): The US Department of Homeland Security
            [STEM Designated Degree Program List](
            https://www.ice.gov/doclib/sevis/pdf/stemList2024.pdf)
    """

    cip = "cip2020"
    original_cip = "cipcode"
    ncses_sci_group = "ncses_sci_group"
    ncses_field_group = "ncses_field_group"
    ncses_detailed_field_group = "ncses_detailed_field_group"
    nsf_broad_field = "nsf_broad_field"
    dhs_stem = "dhs_stem"

    def __format__(self, fmt):
        # Python 3.11 treatment of StrEnum requires overwriting formatting
        return self.value


class NCSESSciGroup(str, Enum):
    """Field values for the [NCSES Science & Engineering Alternate Classification](
            https://ncsesdata.nsf.gov/sere/2018/html/sere18-dt-taba001.html)

    Attributes:
        sci (str): Science and engineering
        non_sci (str): Non-science and engineering
        unknown (str): Not categorized in NCSES crosswalk

    """

    sci = "Science and engineering"
    non_sci = "Non-science and engineering"
    unknown = "Not categorized in NCSES crosswalk"


class NCSESFieldGroup(str, Enum):
    """Field values for the [NCSES Broad Field Alternate Classification](
            https://ncsesdata.nsf.gov/sere/2018/html/sere18-dt-taba001.html)

    Attributes:
        business_and_mgmt (str): Business and Management
        education (str): Education
        sci_eng_technologies (str): Science and Engineering Technologies
        social_sciences (str): Social Sciences
        nonsci_other (str): Other Non-sciences or Unknown Disciplines
        art_music (str): Arts and Music
        humanities (str): Humanities
        math_cs (str): Math and Computer Sciences
        nonsci_life_sci (str): Life Sciences (Non-S&E)
        sci_life_sci (str): Life Sciences
        home_ec (str): Vocational Studies and Home Economics
        engineering (str): Engineering
        phys_sci (str): Physical Sciences
        communication_librarianship (str): Communication and Librarianship
        psych (str): Psychology
        religion (str): Religion and Theology
        interdisc (str): Interdisciplinary or Other Sciences
        social_service (str): Social Service Professions
        geosci (str): Geosciences
        arch_dsgn (str): Architecture and Environmental Design
        law (str): Law
        unknown (str): Not categorized in NCSES crosswalk

    """

    business_and_mgmt = "Business and Management"
    education = "Education"
    sci_eng_technologies = "Science and Engineering Technologies"
    social_sciences = "Social Sciences"
    nonsci_other = "Other Non-sciences or Unknown Disciplines"
    art_music = "Arts and Music"
    humanities = "Humanities"
    math_cs = "Math and Computer Sciences"
    nonsci_life_sci = "Life Sciences (Non-S&E)"
    sci_life_sci = "Life Sciences"
    home_ec = "Vocational Studies and Home Economics"
    engineering = "Engineering"
    phys_sci = "Physical Sciences"
    communication_librarianship = "Communication and Librarianship"
    psych = "Psychology"
    religion = "Religion and Theology"
    interdisc = "Interdisciplinary or Other Sciences"
    social_service = "Social Service Professions"
    geosci = "Geosciences"
    arch_dsgn = "Architecture and Environmental Design"
    law = "Law"
    unknown = "Not categorized in NCSES crosswalk"


class NCSESDetailedFieldGroup(str, Enum):
    """Field values for the [NCSES Detailed Field Alternate Classification](
            https://ncsesdata.nsf.gov/sere/2018/html/sere18-dt-taba001.html)

    Attributes:
        business_and_mgmt (str): Business and Management
        nonsci_edu (str): Non-Science Education
        nonsci_other (str): Other Non-sciences or Unknown Disciplines
        art_music (str): Arts and Music
        life_sci_other (str): Other Life Sciences
        health_technologies (str): Health Technologies
        compsci (str): Computer Science
        bio (str): Biological Sciences
        home_ec (str): Vocational Studies and Home Economics
        engineering_technologies (str): Engineering Technologies
        languages (str): Foreign Languages
        communication_librarianship (str): Communication and Librarianship
        psych (str): Psychology
        polisci (str): Political Science and Public Administration
        english_lit (str): English and Literature
        math (str): Mathematics and Statistics
        ag_sci (str): Agricultural Sciences
        religion (str): Religion and Theology
        social_sci_other (str): Other Social Sciences
        medical_sci (str): Medical Sciences
        history (str): History
        engineering_other (str): Other Engineering
        chem (str): Chemistry
        area_ethnic (str): Area and Ethnic Studies
        sociology (str): Sociology
        humanities_other (str): Other Humanities
        interdisc (str): Interdisciplinary or Other Sciences
        econ (str): Economics
        physics (str): Physics
        social_service (str): Social Service Professions
        electrical_eng (str): Electrical Engineering
        science_edu (str): Science Education
        earth_sci (str): Earth Sciences
        arch_dsgn (str): Architecture and Environmental Design
        law (str): Law
        civil_engineering (str): Civil Engineering
        anthro (str): Anthropology
        phys_sci_other (str): Other Physical Sciences
        mech_e (str): Mechanical Engineering
        math_edu (str): Mathematics Education
        sci_tech_edu (str): Other Science/Technical Education
        chem_e (str): Chemical Engineering
        industrial_eng (str): Industrial Engineering
        socsci_edu (str): Social Science Education
        sci_technologies (str): Science Technologies
        linguistics (str): Linguistics
        materials_engineering (str): Materials Engineering
        astronomy (str): Astronomy
        aerospace_eng (str): Aerospace Engineering
        atmospheric_sciences (str): Atmospheric Sciences
        oceanography (str): Oceanography
        sci_eng_tech_other (str): Other Science and Engineering Technologies
        hist_sci (str): History of Science
        unknown (str): Not categorized in NCSES crosswalk

    """

    business_and_mgmt = "Business and Management"
    nonsci_edu = "Non-Science Education"
    nonsci_other = "Other Non-sciences or Unknown Disciplines"
    art_music = "Arts and Music"
    life_sci_other = "Other Life Sciences"
    health_technologies = "Health Technologies"
    compsci = "Computer Science"
    bio = "Biological Sciences"
    home_ec = "Vocational Studies and Home Economics"
    engineering_technologies = "Engineering Technologies"
    languages = "Foreign Languages"
    communication_librarianship = "Communication and Librarianship"
    psych = "Psychology"
    polisci = "Political Science and Public Administration"
    english_lit = "English and Literature"
    math = "Mathematics and Statistics"
    ag_sci = "Agricultural Sciences"
    religion = "Religion and Theology"
    social_sci_other = "Other Social Sciences"
    medical_sci = "Medical Sciences"
    history = "History"
    engineering_other = "Other Engineering"
    chem = "Chemistry"
    area_ethnic = "Area and Ethnic Studies"
    sociology = "Sociology"
    humanities_other = "Other Humanities"
    interdisc = "Interdisciplinary or Other Sciences"
    econ = "Economics"
    physics = "Physics"
    social_service = "Social Service Professions"
    electrical_eng = "Electrical Engineering"
    science_edu = "Science Education"
    earth_sci = "Earth Sciences"
    arch_dsgn = "Architecture and Environmental Design"
    law = "Law"
    civil_engineering = "Civil Engineering"
    anthro = "Anthropology"
    phys_sci_other = "Other Physical Sciences"
    mech_e = "Mechanical Engineering"
    math_edu = "Mathematics Education"
    sci_tech_edu = "Other Science/Technical Education"
    chem_e = "Chemical Engineering"
    industrial_eng = "Industrial Engineering"
    socsci_edu = "Social Science Education"
    sci_technologies = "Science Technologies"
    linguistics = "Linguistics"
    materials_engineering = "Materials Engineering"
    astronomy = "Astronomy"
    aerospace_eng = "Aerospace Engineering"
    atmospheric_sciences = "Atmospheric Sciences"
    oceanography = "Oceanography"
    sci_eng_tech_other = "Other Science and Engineering Technologies"
    hist_sci = "History of Science"
    unknown = "Not categorized in NCSES crosswalk"


class NSFBroadField(str, Enum):
    """Field values according to the [NSF Diversity in STEM Report](
            https://ncses.nsf.gov/pubs/nsf23315/report)

    Attributes:
        ag_and_bio_sci (str): Agricultural and biological sciences
        math_cs (str): Mathematical and computer sciences
        phys_earth_sci (str): Physical and earth sciences
        soc_behav_sci (str): Social and behavioral sciences
        eng (str): Engineering
        non_stem (str): Non-science and engineering
    """

    ag_and_bio_sci = "Agricultural and biological sciences"
    math_cs = "Mathematical and computer sciences"
    phys_earth_sci = "Physical and earth sciences"
    soc_behav_sci = "Social and behavioral sciences"
    eng = "Engineering"
    non_stem = "Non-science and engineering"


# Hierarchical relationships in the NCSES Alternative Classification
NCSES_HIERARCHY = {
    NCSESSciGroup.unknown: {NCSESFieldGroup.unknown: [NCSESDetailedFieldGroup.unknown]},
    NCSESSciGroup.non_sci: {
        NCSESFieldGroup.arch_dsgn: [NCSESDetailedFieldGroup.arch_dsgn],
        NCSESFieldGroup.art_music: [NCSESDetailedFieldGroup.art_music],
        NCSESFieldGroup.business_and_mgmt: [NCSESDetailedFieldGroup.business_and_mgmt],
        NCSESFieldGroup.communication_librarianship: [
            NCSESDetailedFieldGroup.communication_librarianship
        ],
        NCSESFieldGroup.education: [
            NCSESDetailedFieldGroup.nonsci_edu,
            NCSESDetailedFieldGroup.math_edu,
            NCSESDetailedFieldGroup.sci_tech_edu,
            NCSESDetailedFieldGroup.science_edu,
            NCSESDetailedFieldGroup.socsci_edu,
        ],
        NCSESFieldGroup.humanities: [
            NCSESDetailedFieldGroup.english_lit,
            NCSESDetailedFieldGroup.languages,
            NCSESDetailedFieldGroup.humanities_other,
            NCSESDetailedFieldGroup.history,
        ],
        NCSESFieldGroup.interdisc: [NCSESDetailedFieldGroup.interdisc],
        NCSESFieldGroup.law: [NCSESDetailedFieldGroup.law],
        NCSESFieldGroup.nonsci_life_sci: [
            NCSESDetailedFieldGroup.medical_sci,
            NCSESDetailedFieldGroup.life_sci_other,
        ],
        NCSESFieldGroup.nonsci_other: [NCSESDetailedFieldGroup.nonsci_other],
        NCSESFieldGroup.religion: [NCSESDetailedFieldGroup.religion],
        NCSESFieldGroup.sci_eng_technologies: [
            NCSESDetailedFieldGroup.engineering_technologies,
            NCSESDetailedFieldGroup.sci_technologies,
            NCSESDetailedFieldGroup.sci_eng_tech_other,
            NCSESDetailedFieldGroup.health_technologies,
        ],
        NCSESFieldGroup.home_ec: [NCSESDetailedFieldGroup.home_ec],
        NCSESFieldGroup.social_service: [NCSESDetailedFieldGroup.social_service],
    },
    NCSESSciGroup.sci: {
        NCSESFieldGroup.engineering: [
            NCSESDetailedFieldGroup.engineering_other,
            NCSESDetailedFieldGroup.aerospace_eng,
            NCSESDetailedFieldGroup.chem_e,
            NCSESDetailedFieldGroup.civil_engineering,
            NCSESDetailedFieldGroup.electrical_eng,
            NCSESDetailedFieldGroup.industrial_eng,
            NCSESDetailedFieldGroup.materials_engineering,
            NCSESDetailedFieldGroup.mech_e,
        ],
        NCSESFieldGroup.sci_life_sci: [
            NCSESDetailedFieldGroup.ag_sci,
            NCSESDetailedFieldGroup.bio,
        ],
        NCSESFieldGroup.math_cs: [
            NCSESDetailedFieldGroup.compsci,
            NCSESDetailedFieldGroup.math,
        ],
        NCSESFieldGroup.phys_sci: [
            NCSESDetailedFieldGroup.phys_sci_other,
            NCSESDetailedFieldGroup.astronomy,
            NCSESDetailedFieldGroup.chem,
            NCSESDetailedFieldGroup.physics,
        ],
        NCSESFieldGroup.psych: [NCSESDetailedFieldGroup.psych],
        NCSESFieldGroup.social_sciences: [
            NCSESDetailedFieldGroup.area_ethnic,
            NCSESDetailedFieldGroup.social_sci_other,
            NCSESDetailedFieldGroup.polisci,
            NCSESDetailedFieldGroup.anthro,
            NCSESDetailedFieldGroup.econ,
            NCSESDetailedFieldGroup.linguistics,
            NCSESDetailedFieldGroup.sociology,
            NCSESDetailedFieldGroup.hist_sci,
        ],
        NCSESFieldGroup.geosci: [
            NCSESDetailedFieldGroup.atmospheric_sciences,
            NCSESDetailedFieldGroup.earth_sci,
            NCSESDetailedFieldGroup.oceanography,
        ],
    },
}

# Mapping between NCSES Broad Fields and NSF Broad Fields
NSF_REPORT_BROAD_FIELD_MAP = {
    NCSESFieldGroup.social_sciences: NSFBroadField.soc_behav_sci,
    NCSESFieldGroup.math_cs: NSFBroadField.math_cs,
    NCSESFieldGroup.sci_life_sci: NSFBroadField.ag_and_bio_sci,
    NCSESFieldGroup.engineering: NSFBroadField.eng,
    NCSESFieldGroup.phys_sci: NSFBroadField.phys_earth_sci,
    NCSESFieldGroup.psych: NSFBroadField.soc_behav_sci,
    NCSESFieldGroup.geosci: NSFBroadField.phys_earth_sci,
}


# Predefined publisher profiles for UniSync

PUBLISHER_PROFILES = {
    "PUB-001": {
        "name": "WVSU Administration",
        "topics": {
            "university.announcements",
            "university.academic",
            "university.events",
            "university.emergency",
        },
    },
    "PUB-002": {
        "name": "University Library",
        "topics": {
            "university.library",
        },
    },
    "PUB-003": {
        "name": "University IT Office",
        "topics": {
            "university.it",
        },
    },
    "PUB-004": {
        "name": "College of ICT",
        "topics": {
            "college.ict",
        },
    },
    "PUB-005": {
        "name": "College of Business",
        "topics": {
            "college.business",
        },
    },
    "PUB-006": {
        "name": "College of Engineering",
        "topics": {
            "college.engineering",
        },
    },
    "PUB-007": {
        "name": "College of Education",
        "topics": {
            "college.education",
        },
    },
    "PUB-008": {
        "name": "College of Arts and Sciences",
        "topics": {
            "college.arts_sciences",
        },
    },
    "PUB-009": {
        "name": "College of Nursing",
        "topics": {
            "college.nursing",
        },
    },
    "PUB-010": {
        "name": "College of Agriculture",
        "topics": {
            "college.agriculture",
        },
    },
}


def get_publisher(publisher_id):
    """Returns a registered publisher profile."""
    return PUBLISHER_PROFILES.get(publisher_id)


def get_publisher_topics(publisher_id):
    """Returns the topics assigned to a publisher."""
    publisher = get_publisher(publisher_id)

    if publisher is None:
        return set()

    return publisher["topics"]

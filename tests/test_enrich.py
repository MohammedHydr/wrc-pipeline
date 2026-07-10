"""Unit tests for the curated->enriched structured extraction.

Fixture mirrors the real structure of a WRC adjudication decision page
(labelled Officer/Hearing lines, Act citations, complaint references,
euro award amounts). No network, no DB.
"""

from transform.enrich import extract_decision_fields, split_parties

DECISION_HTML = b"""
<html><body>
  <h1>ADJ-00017470 - Workplace Relations Commission</h1>
  <h2>ADJUDICATION OFFICER DECISION</h2>
  <p>Adjudication Reference: ADJ-00017470</p>
  <p>Complaint seeking adjudication by the Workplace Relations Commission
     under section 77 of the Employment Equality Act, 1998</p>
  <p>CA-00022612-001</p>
  <p>Date of Adjudication Hearing: 18/10/2021</p>
  <p>Workplace Relations Commission Adjudication Officer: Patsy Doyle</p>
  <p>Procedure: In accordance with Section 79 of the Employment Equality
     Acts, 1998 - 2015 ...</p>
  <p>Decision: I find the complaint to be well founded and order the
     Respondent to pay the Complainant \xe2\x82\xac7,500 in compensation.</p>
</body></html>
"""


def test_extracts_officer_and_hearing_date():
    fields = extract_decision_fields(DECISION_HTML)
    assert fields["adjudication_officer"] == "Patsy Doyle"
    assert fields["hearing_date"] == "2021-10-18"


def test_extracts_acts_cited_deduplicated():
    fields = extract_decision_fields(DECISION_HTML)
    assert "Employment Equality Act 1998" in fields["acts_cited"]
    assert "Employment Equality Acts 1998 - 2015" in fields["acts_cited"]
    assert len(fields["acts_cited"]) == len(set(fields["acts_cited"]))


def test_extracts_complaint_references_and_awards():
    fields = extract_decision_fields(DECISION_HTML)
    assert fields["complaint_references"] == ["CA-00022612-001"]
    assert fields["award_amounts_eur"] == [7500.0]
    assert fields["award_max_eur"] == 7500.0


def test_extracts_outcome_signals():
    fields = extract_decision_fields(DECISION_HTML)
    assert "well founded" in fields["outcome_signals"]
    assert "not well founded" not in fields["outcome_signals"]


def test_no_match_yields_nulls_not_guesses():
    fields = extract_decision_fields(b"<html><body><p>hello</p></body></html>")
    assert fields["adjudication_officer"] is None
    assert fields["hearing_date"] is None
    assert fields["acts_cited"] == []
    assert fields["complaint_references"] == []
    assert fields["award_amounts_eur"] == []
    assert fields["award_max_eur"] is None
    assert fields["outcome_signals"] == []


def test_split_parties_from_listing_description():
    assert split_parties("Declan Holden V Ger Brennan Construction") == {
        "complainant": "Declan Holden",
        "respondent": "Ger Brennan Construction",
    }
    assert split_parties("A Worker v A Hotel") == {
        "complainant": "A Worker",
        "respondent": "A Hotel",
    }


def test_split_parties_handles_missing_or_unsplittable():
    assert split_parties(None) == {"complainant": None, "respondent": None}
    assert split_parties("Recommendation on a trade dispute") == {
        "complainant": None,
        "respondent": None,
    }


def test_act_name_cannot_swallow_a_lowercase_sentence_prefix():
    # Regression: seen in EAT decision 57756 — the act capture must start at
    # the Capitalised act name, not at the beginning of the sentence.
    html = (
        b"<html><body><p>This award is subject to the claimant having been in "
        b"employment which is insurable for all purposes under the Social "
        b"Welfare Consolidation Act 2005</p></body></html>"
    )
    fields = extract_decision_fields(html)
    assert fields["acts_cited"] == ["Social Welfare Consolidation Act 2005"]


def test_act_name_keeps_parenthesised_part_and_ignores_heading_glue():
    html = (
        b"<html><body>"
        b"<h2>ADJUDICATION OFFICER Recommendation</h2>"
        b"<p>heard pursuant to the Civil Law and Criminal Law (Miscellaneous "
        b"Provisions) Act, 2020 and referred under the Industrial Relations "
        b"Act 1969</p>"
        b"</body></html>"
    )
    fields = extract_decision_fields(html)
    assert (
        "Civil Law and Criminal Law (Miscellaneous Provisions) Act 2020"
        in fields["acts_cited"]
    )
    assert "Industrial Relations Act 1969" in fields["acts_cited"]
    # The heading lives in its own element/line and must never prefix an act.
    assert not any(
        "ADJUDICATION" in a or "Recommendation" in a for a in fields["acts_cited"]
    )

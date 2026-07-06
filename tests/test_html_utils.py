"""Tests for deterministic legal-document HTML canonicalization."""

from config.html_utils import canonicalize_html


def test_canonical_output_ignores_dynamic_page_chrome() -> None:
    html_first = b"""
    <!doctype html>
    <html>
      <head>
        <title>Decision A &amp; B</title>
        <script nonce="request-111">analytics()</script>
      </head>
      <body>
        <header id="generated-header-1">Website header</header>

        <main id="generated-main-1" class="content page-123">
          <h1 style="color: red">ADJ-00000001</h1>
          <p>
            This is the legal decision involving Employee A
            and Employer B.
          </p>
          <table class="styled-table">
            <tr>
              <th colspan="2">Parties</th>
            </tr>
            <tr>
              <td>Worker</td>
              <td>Employer</td>
            </tr>
          </table>
        </main>

        <footer>Footer generated at request 111</footer>
      </body>
    </html>
    """

    html_second = b"""
    <!doctype html>
    <html>
      <head>
        <title>Decision A &amp; B</title>
        <script nonce="request-999">differentAnalytics()</script>
      </head>
      <body>
        <header id="generated-header-999">Different site header</header>

        <main id="generated-main-999" class="other-class">
          <h1 onclick="track()">ADJ-00000001</h1>
          <p>This is the legal decision involving
             Employee A and Employer B.</p>
          <table data-request-id="999">
            <tr><th colspan="2">Parties</th></tr>
            <tr><td>Worker</td><td>Employer</td></tr>
          </table>
        </main>

        <footer>Footer generated at request 999</footer>
      </body>
    </html>
    """

    first = canonicalize_html(
        html_first,
        selectors=["main"],
        min_text_chars=20,
    )
    second = canonicalize_html(
        html_second,
        selectors=["main"],
        min_text_chars=20,
    )

    assert first.content_bytes == second.content_bytes
    assert first.html_bytes == second.html_bytes
    assert first.selector_used == "main"
    assert second.selector_used == "main"
    assert first.fallback_used is False
    assert second.fallback_used is False


def test_navigation_footer_scripts_and_forms_are_removed() -> None:
    raw = b"""
    <html>
      <head>
        <title>Legal Decision</title>
      </head>
      <body>
        <nav>Navigation should disappear</nav>

        <article>
          <h1>ADJ-00000002</h1>
          <p>
            This is the relevant decision content and it must remain in the
            curated document. It contains sufficient text for extraction.
          </p>
          <form>
            <input name="csrf" value="volatile-token">
            <button>Submit</button>
          </form>
        </article>

        <footer>Footer should disappear</footer>
      </body>
    </html>
    """

    result = canonicalize_html(
        raw,
        selectors=["article"],
        min_text_chars=20,
    )

    output = result.html_bytes.decode("utf-8")

    assert "relevant decision content" in output
    assert "Navigation should disappear" not in output
    assert "Footer should disappear" not in output
    assert "volatile-token" not in output
    assert "<form" not in output
    assert "<script" not in output


def test_fallback_does_not_create_nested_body_element() -> None:
    raw = b"""
    <html>
      <head>
        <title>Fallback Decision</title>
      </head>
      <body id="dynamic-id">
        <div>
          This decision content is stored directly in the body because no
          configured selector exists on this particular source page.
        </div>
      </body>
    </html>
    """

    result = canonicalize_html(
        raw,
        selectors=[".selector-that-does-not-exist"],
        min_text_chars=20,
    )

    output = result.html_bytes.decode("utf-8")

    assert result.fallback_used is True
    assert result.selector_used is None
    assert output.count("<body>") == 1
    assert output.count("</body>") == 1


def test_semantic_table_attributes_are_preserved() -> None:
    raw = b"""
    <html>
      <body>
        <main>
          <p>
            A sufficiently long legal decision paragraph is included here so
            that the configured selector is accepted by the canonicalizer.
          </p>
          <table class="visual-only">
            <tr>
              <th colspan="2" class="heading">Representatives</th>
            </tr>
          </table>
        </main>
      </body>
    </html>
    """

    result = canonicalize_html(
        raw,
        selectors=["main"],
        min_text_chars=20,
    )

    output = result.html_bytes.decode("utf-8")

    assert 'colspan="2"' in output
    assert 'class="heading"' not in output
    assert 'class="visual-only"' not in output
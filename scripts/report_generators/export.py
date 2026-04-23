"""HTML/PDF export for report briefs and technical appendix.

This module provides rendering capabilities for briefs and appendix
to standalone HTML and PDF formats suitable for sharing and printing.

Components:
  - BriefHTMLRenderer: Converts brief JSON to HTML
  - AppendixHTMLRenderer: Converts appendix JSON to HTML
  - PDFGenerator: Converts HTML to PDF (wkhtmltopdf)

Usage:
    from scripts.report_generators.export import render_brief_html, generate_pdf
    
    html = render_brief_html("policy")
    pdf = generate_pdf(html, output_path="policy_brief.pdf")
"""

import json
from pathlib import Path
from typing import Optional
from datetime import datetime
from dataclasses import dataclass

import structlog

log = structlog.get_logger(__name__)


@dataclass
class BriefHTMLRenderer:
    """Renders a report brief from JSON to self-contained HTML."""
    
    brief_id: str
    title: str
    output_path: Optional[str] = None
    
    CSS_TEMPLATE = """
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333;
            background: #fff;
            max-width: 1000px;
            margin: 0 auto;
            padding: 40px 20px;
        }
        
        header {
            border-bottom: 3px solid #2c3e50;
            margin-bottom: 40px;
            padding-bottom: 20px;
        }
        
        h1 {
            color: #2c3e50;
            font-size: 2.5em;
            margin-bottom: 10px;
        }
        
        .subtitle {
            color: #7f8c8d;
            font-size: 1.2em;
            margin-bottom: 10px;
        }
        
        .metadata {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-top: 20px;
            font-size: 0.9em;
            color: #555;
        }
        
        .metadata-item {
            background: #ecf0f1;
            padding: 10px;
            border-radius: 4px;
        }
        
        .metadata-label {
            font-weight: bold;
            color: #2c3e50;
        }
        
        section {
            margin-bottom: 40px;
            page-break-inside: avoid;
        }
        
        h2 {
            color: #34495e;
            font-size: 1.8em;
            border-left: 4px solid #3498db;
            padding-left: 15px;
            margin: 30px 0 15px 0;
        }
        
        h3 {
            color: #555;
            font-size: 1.3em;
            margin: 20px 0 10px 0;
        }
        
        .section-content {
            background: #f9f9f9;
            padding: 15px;
            border-radius: 4px;
            border-left: 3px solid #3498db;
        }
        
        .findings {
            margin-bottom: 20px;
        }
        
        .interpretation {
            background: #e8f4f8;
            border-left-color: #2980b9;
            padding: 15px;
            border-radius: 4px;
        }
        
        .method-gates {
            background: #fff;
            border: 1px solid #ddd;
            border-radius: 4px;
            padding: 20px;
            margin: 20px 0;
        }
        
        .method-gate {
            margin-bottom: 15px;
            padding: 10px;
            background: #f0f7ff;
            border-left: 3px solid #2980b9;
        }
        
        .gate-name {
            font-weight: bold;
            color: #2c3e50;
        }
        
        .gate-details {
            margin-top: 8px;
            margin-left: 10px;
            font-size: 0.95em;
        }
        
        .gate-detail {
            margin: 5px 0;
        }
        
        .gate-label {
            font-weight: 600;
            color: #34495e;
            width: 120px;
            display: inline-block;
        }
        
        footer {
            border-top: 2px solid #ecf0f1;
            margin-top: 50px;
            padding-top: 20px;
            text-align: center;
            color: #7f8c8d;
            font-size: 0.9em;
        }
        
        .disclaimer {
            background: #fffbea;
            border: 1px solid #f0ad4e;
            border-radius: 4px;
            padding: 15px;
            margin: 20px 0;
            font-size: 0.95em;
        }
        
        .disclaimer-title {
            font-weight: bold;
            color: #8a6d3b;
            margin-bottom: 8px;
        }
        
        @media (prefers-color-scheme: dark) {
            body {
                background: #1a1a1a;
                color: #e0e0e0;
            }
            h1, h2 { color: #60b6e0; }
            .section-content { background: #2a2a2a; }
        }
        
        @page {
            size: A4;
            margin: 2cm;
        }
        
        @media print {
            body {
                font-size: 11pt;
            }
        }
    </style>
    """
    
    def render(self, brief_data: dict) -> str:
        """Render brief data to a complete HTML document string."""
        metadata = brief_data.get("metadata", {})
        sections = brief_data.get("sections", {})
        method_gates = brief_data.get("method_gates", [])
        generated_at = brief_data.get("generated_at", "N/A")

        html_parts = [*self._render_html_open(metadata)]
        html_parts.append(self._render_header(metadata, sections, method_gates, generated_at))
        if method_gates:
            html_parts.extend(self._render_method_gates_block(method_gates))
        html_parts.extend(self._render_sections_block(sections))
        html_parts.append(self._render_disclaimer())
        html_parts.append(self._render_footer())
        html_parts.extend(["</body>", "</html>"])
        return "\n".join(html_parts)

    def _render_html_open(self, metadata: dict) -> list[str]:
        """Opening boilerplate: doctype, head, body open tag."""
        return [
            "<!DOCTYPE html>",
            "<html lang='en'>",
            "<head>",
            "<meta charset='utf-8'>",
            f"<title>{metadata.get('title', 'Report Brief')}</title>",
            self.CSS_TEMPLATE,
            "</head>",
            "<body>",
        ]

    def _render_header(
        self,
        metadata: dict,
        sections: dict,
        method_gates: list,
        generated_at: str,
    ) -> str:
        """Title block + 4-cell metadata grid (audience / date / counts)."""
        return f"""
        <header>
            <h1>{metadata.get('title', 'Report Brief')}</h1>
            <p class='subtitle'>{metadata.get('description', '')}</p>

            <div class='metadata'>
                <div class='metadata-item'>
                    <span class='metadata-label'>Audience:</span><br>
                    {metadata.get('audience', 'N/A')}
                </div>
                <div class='metadata-item'>
                    <span class='metadata-label'>Generated:</span><br>
                    {generated_at}
                </div>
                <div class='metadata-item'>
                    <span class='metadata-label'>Sections:</span><br>
                    {len(sections)}
                </div>
                <div class='metadata-item'>
                    <span class='metadata-label'>Method Gates:</span><br>
                    {len(method_gates)}
                </div>
            </div>
        </header>
        """

    def _render_method_gates_block(self, method_gates: list) -> list[str]:
        """Methodology summary section listing each gate."""
        parts = ["<div class='method-gates'>", "<h2>Methodology</h2>"]
        parts.extend(self._render_one_method_gate(g) for g in method_gates)
        parts.append("</div>")
        return parts

    def _render_one_method_gate(self, gate: dict) -> str:
        """One method-gate card (algorithm / validation / null model)."""
        return f"""
                <div class='method-gate'>
                    <div class='gate-name'>{gate.get('name', 'Gate')}</div>
                    <div class='gate-details'>
                        <div class='gate-detail'>
                            <span class='gate-label'>Algorithm:</span>
                            {gate.get('algorithm', 'N/A')}
                        </div>
                        <div class='gate-detail'>
                            <span class='gate-label'>Validation:</span>
                            {gate.get('validation_method', 'N/A')}
                        </div>
                        <div class='gate-detail'>
                            <span class='gate-label'>Null Model:</span>
                            {gate.get('null_model', 'N/A')}
                        </div>
                    </div>
                </div>
                """

    def _render_sections_block(self, sections: dict) -> list[str]:
        """Concatenate every brief section in declaration order."""
        parts: list[str] = []
        for section_id, section_data in sections.items():
            parts.extend(self._render_one_section(section_id, section_data))
        return parts

    def _render_one_section(self, section_id: str, section_data: dict) -> list[str]:
        """One <section> with optional Findings + Interpretation children."""
        title = section_id.replace('_', ' ').title()
        parts = ["<section>", f"<h2>{title}</h2>"]
        findings = section_data.get("findings", "")
        interpretation = section_data.get("interpretation", "")
        if findings:
            parts.append(self._render_findings_block(findings))
        if interpretation:
            parts.append(self._render_interpretation_block(interpretation))
        parts.append("</section>")
        return parts

    def _render_findings_block(self, findings: str) -> str:
        """Findings sub-block (neutral facts layer)."""
        return (
            "<div class='section-content'>"
            "<h3>Findings</h3>"
            f"<div class='findings'>{findings}</div>"
            "</div>"
        )

    def _render_interpretation_block(self, interpretation: str) -> str:
        """Interpretation sub-block (first-person, multi-hypothesis layer)."""
        return (
            "<div class='section-content interpretation'>"
            "<h3>Interpretation</h3>"
            f"<div class='interpretation'>{interpretation}</div>"
            "</div>"
        )

    def _render_disclaimer(self) -> str:
        """Mandatory legal/ethical disclaimer block (English)."""
        return """
        <div class='disclaimer'>
            <div class='disclaimer-title'>Disclaimer</div>
            <p>This report presents analysis of industry collaboration networks based on public credit data.
            Scores measure structural network position, not individual competence or ability.
            All estimates include confidence intervals. See methodology section for complete details.</p>
        </div>
        """

    def _render_footer(self) -> str:
        """Footer with generation timestamp + product label."""
        return f"""
        <footer>
            <p>Generated: {datetime.now().isoformat()}</p>
            <p>Animetor Eval v2 Report System</p>
        </footer>
        """
    
    def render_to_file(self, brief_data: dict, output_path: str) -> bool:
        """Render brief to HTML file.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            html = self.render(brief_data)
            Path(output_path).write_text(html, encoding='utf-8')
            log.info("brief_html_written", file=output_path, size_kb=len(html) / 1024)
            return True
        except Exception as e:
            log.exception("brief_html_write_error", file=output_path, error=str(e))
            return False


def render_brief_html(brief_id: str, output_dir: str = "result/html") -> Optional[str]:
    """Render a brief from JSON to HTML.
    
    Args:
        brief_id: 'policy', 'hr', or 'business'
        output_dir: Directory for HTML output
    
    Returns:
        Path to output HTML file, or None if failed
    """
    json_path = f"result/json/{brief_id}_brief.json"
    output_path = f"{output_dir}/{brief_id}_brief.html"
    
    try:
        with open(json_path) as f:
            brief_data = json.load(f)
        
        renderer = BriefHTMLRenderer(brief_id=brief_id, title=brief_data.get("metadata", {}).get("title", brief_id))
        
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        if renderer.render_to_file(brief_data, output_path):
            return output_path
    
    except Exception as e:
        log.exception("render_brief_error", brief_id=brief_id, error=str(e))
    
    return None


# Placeholder for PDF generation (wkhtmltopdf integration)
def generate_pdf_from_html(html_path: str, pdf_path: str) -> bool:
    """Generate PDF from HTML file using wkhtmltopdf.
    
    Args:
        html_path: Path to HTML file
        pdf_path: Path to output PDF
    
    Returns:
        True if successful, False otherwise
    
    Note:
        Requires wkhtmltopdf system package installed.
        Install with: apt-get install wkhtmltopdf
    """
    try:
        import subprocess
        
        result = subprocess.run(
            ["wkhtmltopdf", html_path, pdf_path],
            capture_output=True,
            timeout=30
        )
        
        if result.returncode == 0:
            log.info("pdf_generated", file=pdf_path)
            return True
        else:
            log.error("pdf_generation_failed", stderr=result.stderr.decode())
            return False
    
    except FileNotFoundError:
        log.warning("wkhtmltopdf_not_found", hint="Install with: apt-get install wkhtmltopdf")
        return False
    except Exception as e:
        log.exception("pdf_generation_error", error=str(e))
        return False

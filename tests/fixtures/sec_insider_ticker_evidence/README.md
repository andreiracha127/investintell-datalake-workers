# SEC insider evidence fixtures

DERA slices contain the complete unmodified source fields for the accessions cited
in section 5 of `W1B-INSIDER-FEASIBILITY.md`. Headers follow their source quarter,
including `AFF10B5ONE` where present. CSV escaping and LF line endings are
canonicalized; field values are unchanged.

The May 2003 ownership XML and archive metadata come from sec-api original filing
archives. The Form 3 specimen exercises X0101 and a lowercase OTC symbol; the Form
5 specimen exercises `EDLG, OB` and its actual EDGAR acceptance timestamp. Two
additional May 2003 Form 3/3A specimens have malformed `documentType` elements;
the correct form comes from their EDGAR archive metadata. XML content is
unchanged apart from LF line endings. `provenance.json` pins source
URLs, archive members, accession numbers, and original XML SHA-256 hashes.

Resolution tests use controlled synthetic filing histories to isolate admission,
conflict, date-boundary, and package-correction behavior. No present-day CIK prior
is consulted.

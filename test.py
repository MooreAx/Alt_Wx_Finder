import re

raw = "TEMPO 0116/0123 5SM -SN BR VCTSRA OVC012"

sigwx_pattern = re.compile(
    r'(?:(?<=^)|(?<=\s))'					#look behind: start with new line or space (do not consume)
    r'(?:[\+\-−–]|VC)?'						# optional intensity / proximity
    r'(?:MI|BC|PR|DR|BL|SH|TS|FZ)?'			# optional descriptor
    r'(?:DZ|RA|SN|SG|IC|PL|GR|GS|BR|FG|FU|DU|SA|HZ|VA|PO|SQ|\+?FC|\+?SS|\+?DS)\b'  # phenomenon
)
matches = sigwx_pattern.findall(raw)
print(matches)  # Should print: ['-SN', 'BR']
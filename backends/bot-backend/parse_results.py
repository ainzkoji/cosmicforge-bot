import xml.etree.ElementTree as ET

tree = ET.parse('report.xml')
for tc in tree.findall('.//testcase'):
    failure = tc.find('failure')
    if failure is not None:
        lines = failure.get('message').splitlines()
        first_line = lines[-1] if lines else "Unknown error"
        print(f"{tc.get('classname')}.{tc.get('name')}: {first_line}")

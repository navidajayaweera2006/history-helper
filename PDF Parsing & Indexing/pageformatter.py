with open("yourfile.md", "r", encoding="utf-8") as file:
    content = file.read()

pages = content.split('pageseparator')

numbered_pages = []
for i, page in enumerate(pages, start=1):
    header = f"Page Number - {i}\n\n"
    numbered_pages.append(header + page.strip())

new_content = '\n\npageseparator\n\n'.join(numbered_pages)

with open("numbered.md", "w", encoding="utf-8") as file:
    file.write(new_content)

print("Page numbers added.")

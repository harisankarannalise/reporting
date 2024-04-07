import re

if __name__ == "__main__":
    import xlsxwriter

    workbook = xlsxwriter.Workbook("rich_strings.xlsx")
    worksheet = workbook.add_worksheet()

    worksheet.set_column("A:A", 30)

    # Set up some formats to use.
    bold = workbook.add_format({"bold": True})
    italic = workbook.add_format({"italic": True})
    red = workbook.add_format({"color": "red"})
    blue = workbook.add_format({"color": "blue"})
    center = workbook.add_format({"align": "center"})
    superscript = workbook.add_format({"font_script": 1})

    text = """
        Title	CHEST X-RAY
        
        Projection:	Frontal chest radiograph.
        
        Findings:		
        Technical quality:	Satisfactory image quality.

        Lines and Tubes:	No lines or tubes identified.
        
        Cardiomediastinal:	<b>Thoracic aortic calcification demonstrated. Aorta is tortuous.</b>
        
        Lung parenchyma:	<b>Upper zone predominant bilateral reticular changes without volume loss.</b>
        
        Pleural space:	Normal pleural spaces.

        Bones:	No bony abnormality.
        Other:	<b>Surgical clips present in the abdomen.</b>
        """

    # # Split the text by the bold tags
    # parts = text.split('<b>')
    # first_part = parts[0]
    # for part in parts[1:]:
    #     bold_part, rest = part.split('</b>', 1)
    #     # # Write the first part without bold formatting
    #     # worksheet.write_rich_string('A1', None, first_part)
    #     # # Write the bold part
    #     # worksheet.write_rich_string('A1', bold_part, workbook.add_format({'bold': True}))
    #     # # Update the first part for next iteration
    #     # first_part = rest
    #
    # # Write some strings with multiple formats.
    # worksheet.write_rich_string(
    #     "A1", first_part, bold, bold_part, rest
    # )
    #
    # worksheet.write_rich_string("A3", "This is ", red, "red", " and this is ", blue, "blue")
    #
    # worksheet.write_rich_string("A5", "Some ", bold, "bold text", " centered", center)
    #
    # worksheet.write_rich_string("A7", italic, "j = k", superscript, "(n-1)", center)
    #
    # # If you have formats and segments in a list you can add them like this:
    # segments = ["This is ", bold, "bold", " and this is ", blue, "blue"]
    # worksheet.write_rich_string("A9", *segments)
    #
    # workbook.close()

    # Find indices of <b> and </b> tags using regular expressions
    # Find indices of <b> and </b> tags using regular expressions
    b_indices = [(match.start(), match.end()) for match in re.finditer(r'<b>', text)]
    end_b_indices = [(match.start(), match.end()) for match in re.finditer(r'</b>', text)]

    # Pair up <b> and </b> indices
    paired_indices = [(b_start, end_b_indices[i][1]) for i, b_start in enumerate(b_indices)]

    print(paired_indices)

    start = 0
    for pair in paired_indices:
        bold_text = text[pair[0] + 3:pair[1] - 4]  # Extract text between <b> and </b>
        worksheet.write(start, 0, text[start:pair[0]])  # Write text before <b>
        worksheet.write_rich_string(start, 0, bold, bold_text)  # Write bold text
        start = pair[1]  # Update start index for next iteration
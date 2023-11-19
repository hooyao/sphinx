import argparse
import fitz


def remove_watermark(source_pdf, out_pdf):
    document = fitz.open(source_pdf)
    for page in document:
        xref = page.get_contents()[0]  # get xref of resulting /Contents object
        cont = bytearray(page.read_contents())  # read the contents source as a (modifyable) bytearray
        if cont.find(b"/Subtype /Watermark") > 0:  # this will confirm a marked-content watermark is present
            while True:
                i1 = cont.find(b"/Artifact")  # start of definition
                if i1 < 0:
                    break  # none more left: done
                i2 = cont.find(b"EMC", i1)  # end of definition
                cont[i1 - 1: i2 + 3] = b""  # remove the full definition source "q ... EMC"
                document.update_stream(xref, cont)  # replace the original source
    document.ez_save(out_pdf)  # save to new file


def main():
    parser = argparse.ArgumentParser(description='Remove watermark from PDF.')
    parser.add_argument('-s', '--source', type=str, required=True, help='Source PDF file')
    parser.add_argument('-o', '--output', type=str, required=True, help='Output PDF file')
    args = parser.parse_args()

    remove_watermark(args.source, args.output)


if __name__ == "__main__":
    main()

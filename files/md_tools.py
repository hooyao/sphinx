import os

import mistune
from bs4 import BeautifulSoup
from pathlib import Path
import shutil

WIKI_DIR = os.path.join(os.getcwd(), 'wiki')

markdown = mistune.Markdown()


def walk_dir(directory: str) -> None:
    for root, subdirs, files in os.walk(directory):
        md_files = [file for file in files if os.path.splitext(file)[1] in ('.md', '.markdown')]
        for md_file in md_files:
            md_file_path = os.path.join(root, md_file)
            with open(md_file_path, 'r+') as f:
                content = f.read()
                md_html = markdown.render(content)
                soup = BeautifulSoup(md_html, 'html.parser')
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    if href.startswith('/'):
                        clean_href = href[1:]
                        href_path = os.path.join(WIKI_DIR, clean_href)
                    elif href.startswith('.'):
                        href_path = os.path.join(root, href)
                    else:
                        href_path = os.path.join(root, href)
                    if os.path.exists(href_path + '.md'):
                        real_md_path = os.path.abspath(href_path + '.md')
                    elif os.path.exists(href_path + '.markdown'):
                        real_md_path = os.path.abspath(href_path + '.markdown')
                    else:
                        continue
                    rel_real_md_path = os.path.relpath(real_md_path, root)
                    content = content.replace(f'({href})', f'({rel_real_md_path})')

                f.seek(0)
                f.write(content)
                f.truncate()
        for sub_dir in subdirs:
            walk_dir(os.path.join(root, sub_dir))


def convert_to_html(source_md_path: str, dest_html_dir: str) -> str:
    file_base_name = os.path.splitext(os.path.basename(source_md_path))[0]
    html_abs_path = os.path.join(dest_html_dir, file_base_name + '.html')

    dest_html_dir_path = Path(dest_html_dir)
    if not os.path.exists(dest_html_dir_path):
        dest_html_dir_path.mkdir(parents=True)
    if os.path.exists(html_abs_path):
        return html_abs_path
    source_dir = os.path.dirname(source_md_path)
    with open(source_md_path, 'r+') as f:
        content = f.read()

    md_html = markdown.parse(content)
    with open(html_abs_path, 'w+') as html_file:
        html_file.write(md_html)

    soup = BeautifulSoup(md_html, 'html.parser')
    md_file_rel_paths = [a['href'] for a in soup.find_all('a', href=True)
                         if os.path.splitext(a['href'])[1] in ('.md', '.markdown')]
    for href in md_file_rel_paths:
        href_abs_path = os.path.abspath(os.path.join(source_dir, href))
        if os.path.exists(href_abs_path):
            rel_md_path = os.path.relpath(href_abs_path, source_dir)
            abs_html_path = os.path.abspath(os.path.join(dest_html_dir, os.path.dirname(rel_md_path)))
            real_html_path = convert_to_html(href_abs_path, abs_html_path)
            rel_real_html_path = os.path.relpath(real_html_path, dest_html_dir)
            with open(html_abs_path, 'r+') as html_file:
                html_content = html_file.read()
                html_content = html_content.replace(f'href="{href}"', f'href="{rel_real_html_path}"')
                html_file.seek(0)
                html_file.write(html_content)
                html_file.truncate()
        else:
            continue

    resource_file_rel_paths = [img['src'] for img in soup.find_all('img')
                               if os.path.splitext(img['src'])[1] in ('.png',)]
    for href in resource_file_rel_paths:
        href_abs_path = os.path.abspath(os.path.join(source_dir, href))
        if os.path.exists(href_abs_path):
            rel_md_path = os.path.relpath(href_abs_path, source_dir)
            abs_html_dir_path = os.path.abspath(os.path.join(dest_html_dir, os.path.dirname(rel_md_path)))
            if not os.path.exists(abs_html_dir_path):
                p = Path(abs_html_dir_path)
                p.mkdir(parents=True)
            shutil.copy(href_abs_path,os.path.join(abs_html_dir_path))
        else:
            continue
    return html_abs_path


# walk_dir(WIKI_DIR)
converted_md_set = set()
convert_to_html(os.path.join(WIKI_DIR, 'home.md'), os.path.join(WIKI_DIR, 'html'))

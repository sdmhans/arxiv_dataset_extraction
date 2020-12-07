#coding: utf-8
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextBox, LTChar, LAParams
import re
import os
import glob
import json

from functools import partial
from p_tqdm import p_map
from tqdm import tqdm

from multiprocessing import Pool
TIMELIMIT = 2*60

meta_dict = {}
with open("arxiv-metadata-oai-snapshot.json", 'r', encoding='utf-8') as f:
    for line in tqdm(f.readlines()):
        file_meta_data = json.loads(line)
        file_dic = {}
        file_dic['id'] = file_meta_data['id']
        
        file_dic['title'] = ' '.join([x.strip() for x in file_meta_data['title'].split('\n')]).strip()
        file_dic['abstract'] = ' '.join([x.strip() for x in file_meta_data['abstract'].split('\n')]).strip()
        
        meta_dict[file_dic['id']] = file_dic

# extract text from each section
def clean_section(section_content):
    # remove reference symbols
    for i, line in enumerate(section_content):
        line = re.sub('\[[0-9]+\]', '', line)
        line = re.sub('\[[0-9]+(, [0-9]+)+\]', '', line)
        line = re.sub('\\x0b', '', line)
        line = re.sub('\\x0c', '', line)
        if line == '':
            line = '\n'
        section_content[i] = line
    
    cleaned_txt = []
    # remove a line of text if it contains too many mathematical symbols
    for i, line in enumerate(section_content):
        cnt = 0
        for digit in line.lower():
            if digit not in set([chr(ord('a')+i) for i in range(26)]+['%']):
                cnt += 1
        if cnt / len(line) <= 0.65:
            if line.lower().startswith('e-mail') or line.lower().startswith('email') or line.lower().startswith('mail'):
                line = '\n'
            else:
                line = re.sub(r'[^a-zA-Z0-9()&%$#\s><\+\*/,\.:;\'\"?!=_-]', '', line)
            cleaned_txt.append(line)
        else:
            cleaned_txt.append('\n')

    # remove a line of text if it has less than 2 characters(excluding \n)
    for i, x in enumerate(cleaned_txt):
        if len(x) <= 4:
            cleaned_txt[i] = '\n'
    
    # remove a line of text if it is surrounded by \n 
    # (possibly text from equations or tables based on the extraction rules of pdfminer)
    for i, x in enumerate(cleaned_txt):
        if i-1 > 0 and cleaned_txt[i-1] == '\n' and i+1 < len(cleaned_txt) and cleaned_txt[i+1]=='\n':
            cleaned_txt[i] = '\n'
    for i, x in enumerate(cleaned_txt):
        if cleaned_txt[i] == '\n' and i+1 < len(cleaned_txt) and cleaned_txt[i+1] != '\n' \
            and i+2 < len(cleaned_txt) and cleaned_txt[i+2] != '\n' and i+3 < len(cleaned_txt) and cleaned_txt[i+3] == '\n':
            cleaned_txt[i+1] = '\n'
            cleaned_txt[i+2] = '\n'
    
    cleaned_txt = [x for x in cleaned_txt if x != '\n']

    # combining different lines of text based on '.' or '-'
    combined_txt_list = []
    preserved_text = ' '
    for i, x in enumerate(cleaned_txt):
        x = x.strip('\n')
        if preserved_text[-1] == '.':
            combined_txt_list.append(preserved_text)
            preserved_text = x
        elif preserved_text[-1] == '-':
            preserved_text = preserved_text[:-1] + x
        else:
            preserved_text = preserved_text + ' ' + x

    if len(preserved_text) > 0:
        combined_txt_list.append(preserved_text)
    combined_txt_list = [x.strip(' ') for x in combined_txt_list]
    
    return '\n'.join(combined_txt_list)

def convert(path):
    # print(path)
    # some pdfs cannot be extracted by pdfminer
    file_id = '.'.join(path.split('/')[-1].split('.')[:2]).split('v')[0]
    if file_id in ['0807.4277', '1002.4714', '1009.3925', '1604.06700',
        '1908.11021', '1306.4535', '1812.10107']:
        print('skiping...', path)
        section_content_dict = {}
        for k, v in meta_dict[file_id].items():
            section_content_dict[k] = v
        section_content_dict['status'] = 'pdfminer bug!'
        return section_content_dict

    section_lines = []
    contents = []
    all_contents = []

    intro_flag = False
    intro_size = None
    intro_font = None

    reference_flag = False
    try:
        for page in extract_pages(path):
            for text_box in page:
                if isinstance(text_box, LTTextBox):
                    for line in text_box:
                        # https://github.com/pdfminer/pdfminer.six/issues/426
                        line_text = re.sub('ﬁ', 'fi', line.get_text())
                        line_text = re.sub('ﬂ', 'fl', line_text)
                        line_text = re.sub('ﬀ', 'ff', line_text)
                        line_text = re.sub('ﬃ', 'ffi', line_text)

                        # recognizing the introduction section
                        if not intro_flag and line.get_text().strip('\n').strip('.').lower().endswith('introduction'): 
                            for c in line:
                                if isinstance(c, LTChar):
                                    intro_size = int(round(c.size))
                                    intro_font = c.fontname
                                    break
                            intro_flag = True

                        if intro_flag:
                            # we skip the text from the reference section
                            if len(line_text) <= len('references\n')+5 and \
                                    line_text.split(' ')[-1].split('.')[-1].lower() in set(['references\n', 'reference\n']):
                                reference_flag = True
                                break

                            # if a line has the same type of font name and font size as the introduction line, 
                            # and the first letter of each constituting word are capitalized,
                            # we regard it as a new section title
                            for c in line:
                                if isinstance(c, LTChar):
                                    cur_size = int(round(c.size))
                                    cur_font = c.fontname
                                    break

                            if cur_font == intro_font and cur_size == intro_size and len(line_text) > 3:
                                # print(line_text, intro_font, intro_size, cur_size, cur_font)

                                # skipping special symbols(possibly the serial number) in front of the text
                                start_idx = -1
                                for idx, c in enumerate(line_text):
                                    if c.isalpha():
                                        break
                                    else:
                                        start_idx = idx
                                line_text = line_text[start_idx+1:].strip('\n').strip('.')

                                # section titles are split into multiple lines, combine them
                                if len(section_lines) > 0 and len(contents) == section_lines[-1][1] + 1:
                                    prev_line = section_lines[-1][0].strip('\n')
                                    if len(prev_line) > 0 and prev_line[-1] == '-':
                                        line_text = prev_line[:-1] + line_text
                                    else:
                                        line_text = prev_line + ' ' + line_text

                                    section_lines.pop()
                                    contents.pop()
                                    all_contents.pop()                            
                                else:
                                    # make sure the first letter of each word are capitalized
                                    words = line_text.split(' ')
                                    flag = True
                                    for w in words:
                                        if w in ['as', 'for', 
                                                 'in', 'into', 'on', 'onto', 
                                                 'via','with','through', 'by',
                                                 'and', '&', 'or', 'of', 'about', 'besides', 'except', 'the', 'to'] or len(w) < 1 or w[0].isdigit():
                                            continue
                                        if w[0] not in set([chr(ord('A')+i) for i in range(26)]) or not w[0].isupper():
                                            flag=False
                                            break
                                if not reference_flag and flag:
                                    section_lines.append([line_text, len(contents)])

                            if not reference_flag:
                                contents.append(line_text)
                        all_contents.append(line_text)

        section_lines.append(['end-of-paper', len(contents)])

        # finding metadata(id, title, abstract)
        section_content_dict = {}
        file_id = '.'.join(path.split('/')[-1].split('.')[:2]).split('v')[0]
        for k, v in meta_dict[file_id].items():
            section_content_dict[k] = v

        for sec_id, section in enumerate(section_lines[:-1]):
            title = section[0].lower()
            # divide contents by section titles
            section_content = contents[section_lines[sec_id][1]+1:section_lines[sec_id+1][1]]
            section_content_dict[title] = clean_section(section_content)

        section_content_dict['all_contents'] = clean_section(all_contents)
        # print(path)
        # print([(x, len(section_content_dict[x])) for x in section_content_dict.keys()]) 
        
    except Exception as e:
        print(e)
        section_content_dict = {}
        file_id = '.'.join(path.split('/')[-1].split('.')[:2]).split('v')[0]
        for k, v in meta_dict[file_id].items():
            section_content_dict[k] = v        
        section_content_dict['status'] = 'pdfminer bug!'
    return section_content_dict

if __name__ == '__main__':
    full_txt_list = []
    # dir_list = [str(y).rjust(4,'0') for y in sorted([int(x) for x in os.listdir('pdf')], reverse=True)]
    dir_list = [str(y).rjust(4,'0') for y in sorted([int(x) for x in os.listdir('pdf')])]
    if not os.path.exists('json'):
        os.mkdir('json')
    for txt_dir in dir_list:
        print('pdf/{}'.format(txt_dir))
        #if txt_dir in ['1009', '0807', '1002']: continue
        #if int(txt_dir) < 1300: continue
        if os.path.exists('json/{}.json'.format(txt_dir)):
            continue

        pdf_list = ['pdf/{}/{}'.format(txt_dir, x) for x in os.listdir('pdf/'+txt_dir) if x.endswith('.pdf')]
        #try:
        result = p_map((partial(convert)), pdf_list, num_cpus=16)

        # output json
        with open('json/{}.json'.format(txt_dir), 'w', encoding='utf-8') as f:
            for i, x in enumerate(result):
                json.dump(x, f, ensure_ascii=False)
                f.write('\n')
        #except Exception as e:
        #    print(repr(e))
        #    continue


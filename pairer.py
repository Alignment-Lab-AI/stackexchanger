import traceback
import xml.etree.ElementTree as etree
from collections import defaultdict
from bs4 import BeautifulSoup
from tqdm import tqdm
from utils import *
import pyarrow as pa
import pyarrow.parquet as pq
from multiprocessing import Pool
from multiprocessing import cpu_count
import yaml

def default_question():
    return None

class QA_Pairer():

    def __init__(self, xml_path, name=None, out_folder="out", min_score=0, max_responses=None, out_format="parquet"):
        self.xml_path = xml_path
        self.name = name or os.path.basename(os.path.dirname(xml_path))
        self.out_folder = out_folder
        self.min_score = min_score
        self.max_responses = max_responses
        assert out_format == "parquet", "Only parquet output format is supported"
        self.out_format = out_format
        self.questions = {}
        self.question_index = 0
        self.index = 0
        
        self.question_data = []
        self.answer_data = []
        self.score_data = []
        self.is_accepted_data = []
        self.xml_path_data = []
        self.question_index_data = []

    def process(self):
        os.makedirs(self.out_folder, exist_ok=True)
        print(f"Output directory created: {self.out_folder}")
        
        pool = Pool(cpu_count())
        results = pool.imap(self.process_element, etree.iterparse(self.xml_path, events=('end',)), chunksize=100)
        
        for result in tqdm(results, desc=f"Parsing {self.name} XML file"):
            if result:
                question_index, question_text, answer_text, score, is_accepted, xml_path = result
                self.question_data.append(question_text)
                self.answer_data.append(answer_text)
                self.score_data.append(score)
                self.is_accepted_data.append(is_accepted)
                self.xml_path_data.append(xml_path)
                self.question_index_data.append(question_index)
                self.index += 1
                
        self.write_to_parquet()


    def process_element(self, element):
        event, elem = element
        if elem.tag == "row":
            try:
                attribs = defaultdict(lambda: None, elem.attrib)
                if is_question(attribs):
                    trim_attribs(attribs, "question")
                    self.questions[attribs["Id"]] = attribs
                elif is_answer(attribs):
                    result = self.process_answer(attribs)
                    if result:
                        return result
            except:
                traceback.print_exc()
        return None

            
    def process_answer(self, a_attribs):
        assert is_answer(a_attribs), "Must be an answer to process"
        parent_id = a_attribs["ParentId"]
        parent = self.questions.get(parent_id)
        
        if parent:
            question_text = ""
            if parent["Title"]:
                question_text += f'{BeautifulSoup(parent["Title"], "lxml").get_text()}\n\n'
            if parent["Body"]:
                question_text += f'{BeautifulSoup(parent["Body"], "lxml").get_text()}\n\n'
            
            answer_text = BeautifulSoup(a_attribs["Body"], "lxml").get_text()
            score = int(a_attribs["Score"])
            is_accepted = a_attribs["Id"] == parent.get("AcceptedAnswerId")
            xml_path = self.xml_path
            
            return self.question_index, question_text, answer_text, score, is_accepted, xml_path
        
        self.question_index += 1
        
        return None
    
    def write_to_parquet(self):
        print(f"Question data length before writing: {len(self.question_data)}")
        if self.question_data:
            print(f"Question data length: {len(self.question_data)}")
            print(f"Answer data length: {len(self.answer_data)}")
            print(f"Score data length: {len(self.score_data)}")
            print(f"Is accepted data length: {len(self.is_accepted_data)}")
            print(f"XML path data length: {len(self.xml_path_data)}")
            print(f"Question index data length: {len(self.question_index_data)}")
            
            try:
                table = pa.Table.from_arrays([
                    pa.array(range(self.index)),
                    pa.array(self.question_index_data),
                    pa.array(self.question_data),
                    pa.array(self.answer_data),
                    pa.array(self.score_data),
                    pa.array(self.is_accepted_data),
                    pa.array(self.xml_path_data)
                ], names=['index', 'question_index', 'question', 'answer', 'score', 'is_accepted', 'xml_path'])
            
                output_dir = self.out_folder
                os.makedirs(output_dir, exist_ok=True)
                parquet_path = os.path.join(output_dir, f"{self.name}.parquet")
                pq.write_table(table, parquet_path)
                print(f"Parquet file saved at: {parquet_path}")
            
                yaml_path = os.path.join(output_dir, "config.yaml")
                yaml_data = {'xml_paths': [self.xml_path]}
                with open(yaml_path, 'w') as f:
                    yaml.dump(yaml_data, f)
                print(f"YAML config saved at: {yaml_path}")
            except Exception as e:
                print(f"Error writing Parquet file: {str(e)}")
                traceback.print_exc()


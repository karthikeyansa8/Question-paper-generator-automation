import re 
import os
try:
    from lapis_uploader import db_con
    from lapis_uploader.download_file import download_file
    from lapis_uploader import mino_handler
    
except ImportError:
    import db_con
    from download_file import download_file
    import mino_handler
import pandas as pd
import json
import traceback
import sys

output_df = pd.DataFrame(columns=['Question Tag',"Question Text",'Option A','Option B','Option C'])

file_dict = {
    "sample":"https://www.overleaf.com/project/624d2a33fd0e0d4fdf98a09f",
    "C5M":"https://www.overleaf.com/8161568273tvbdrhxsdvzz#8a1363",
    "C5S":"https://www.overleaf.com/8161568273tvbdrhxsdvzz#8a1363",
    "C6M":"https://www.overleaf.com/project/65179b5ff337f4ece82106a8",
    "C6S":"https://www.overleaf.com/project/651811137a78cba746f220c8",
    "C7M":"https://www.overleaf.com/project/6519a860a690ef9fd25883ee",
    "C7S":"https://www.overleaf.com/project/6519a870e39c8ffe50ae6c57",
    "C8M":"https://www.overleaf.com/project/6519a87e7074fb67bd97eba9",
    "C8S":"https://www.overleaf.com/project/6519a8926376647c353bbb7d",
}


def  sync_latex_from_overleaf_to_database(file_name):
    data = {}
    
    download_file(file_name=file_name,file_url=file_dict[file_name])
    data = proceess_files(file_name=file_name)
    upload_to_minio(folder_name=file_name)
    
    return data

def upload_to_minio(folder_name):
    client = mino_handler.create_client()
    for dir, dirs, files in os.walk(f"lapis_uploader/overleaf_files/{folder_name}"):
        for file in files:
            if file.endswith('.png') or file.endswith('.jpg') or file.endswith('.jpeg'):
                try:
                    print(file)
                    client.fput_object(bucket_name='lapis-question-paper-images',file_path=f"lapis_uploader/overleaf_files/{folder_name}/{file}",object_name=f"{folder_name}/{file}")
                except Exception as err:
                    print(err)
                    raise Exception("failed to move files")



def update_to_database(lapis_question_tag,raw_latex_data,cursor,option_json,question_text,question_number,correct_answer,base_chapter_tag):
    query = f"""insert into content.question_pool (question_tag,question_data,base_chapter_tag,question_sub_type_id,question_number,raw_latex_text) 
    values (%(question_tag)s,%(question_data)s,%(base_chapter_tag)s,%(question_sub_type_id)s,%(question_number)s,%(raw_latex_text)s)
    on conflict (question_tag) do update 
    set question_data = EXCLUDED.question_data,
    question_number = EXCLUDED.question_number,
    base_chapter_tag = EXCLUDED.base_chapter_tag,
    raw_latex_text = EXCLUDED.raw_latex_text
    """
    
    query2 = f"""insert into lapis.lapis_question_id (question_id,dt_or_wb) values (%(question_id)s,'dt')
    on conflict (question_id) do nothing"""

    arguments={
        "question_tag":lapis_question_tag,
        "question_data":json.dumps(
            {
                'raw_latex_data':raw_latex_data,
                "option_json":option_json,
                "question_text":question_text,
                "correct_answer":correct_answer
             }
            ,indent=4),
        "question_number":question_number,
        "base_chapter_tag":base_chapter_tag,
        "question_sub_type_id":6,
        "raw_latex_text":raw_latex_data
    }

    db_con.excute_query(query=query,args=arguments)
    db_con.excute_query(query=query2,args={"question_id":lapis_question_tag,
})


def get_options(raw_question_text):
    try:
        optiona = re.findall(pattern=r"optionA=\{([^}]+)}",string=raw_question_text)
    except:
        raise Exception("Option A not detected")
    
    try:
        optionb = re.findall(pattern=r"optionB=\{([^}]+)}",string=raw_question_text)
    except:
        raise Exception("Option B not detected")
    
    try:
        optionc = re.findall(pattern=r"optionC=\{([^}]+)}",string=raw_question_text)
    except:
        raise Exception("Option C not detected")
    
    try:
        optiond = re.findall(pattern=r"optionD=\{([^}]+)}",string=raw_question_text)
    except:
        raise Exception("Option D not detected")

    options = {
        "option_a":optiona,
        "option_b":optionb,
        "option_c":optionc,
        "option_d":optiond,
    }

    return options

def proceess_files(file_name):

    conn,cursor = db_con.create_connection()

    with open(f'lapis_uploader/overleaf_files/{file_name}/main.tex','r',encoding='utf-8') as file:
    # with open(f'lapis_uploader/overleaf_files/{file_name}/Class 7.tex','r') as file:
        data = file.read()
    

    result_list = re.findall(pattern=r'%(\s*)start-of-question\n(.*?)\n%(\s*)end-of-question',string=data,flags=re.DOTALL)

    sucessfully_uploaded = 0
    failed_to_upload = 0
    failed_list = []

    for each_question in result_list:
    
        try:
            raw_question_text = each_question[1]
         

            try:

                lapis_question_tag:str = re.findall(pattern=r"questionTag(\s)*=(\s)*\{(.+?)\n?\}",string=raw_question_text)[0][-1]

                lapis_question_tag = lapis_question_tag.replace("–","-")

                base_chapter_tag = re.findall(pattern=r"[A-Z][5-8][BMS]\d{1,2}",string=lapis_question_tag)

                if len(base_chapter_tag) == 0:
                    base_chapter_tag = base_chapter_tag = re.findall(pattern=r"[A-Z][5-8][B][M]",string=lapis_question_tag)

                if len(base_chapter_tag) == 0:
                    base_chapter_tag = base_chapter_tag = re.findall(pattern=r"[A-Z][5-8][B][MS]",string=lapis_question_tag)

                print("lapis_question_tag",lapis_question_tag,"base_chapter_tag",base_chapter_tag)
                base_chapter_tag = base_chapter_tag[0]

                question_type = lapis_question_tag.split('-')[1]

                print(lapis_question_tag)

            except:
                
                raise Exception("Question ID not detected")
            
            # if question_type.strip().lower() != 'ct':
            options = get_options(raw_question_text=raw_question_text[0])
            
            
            try:
                question_text = re.findall(pattern=r"questiontext(\s)*=(\s)*\{([^}]+)}",string=raw_question_text)[0][-1]
            except:
                raise Exception("Question Not Found")    
            
            try:
                correct_answer = re.findall(pattern=r"correctoption(\s)*=(\s)*\{(.+?)\}",string=raw_question_text)[0][-1]
            except:
                raise Exception("Correct Answe Not Found")    
            
            try:
                question_number = re.findall(pattern=r"questionnumber(\s)*=(\s)*\{(.+?)\n?\}",string=raw_question_text)[0][-1]
            except:
                raise Exception("Question Nnumber not Found")   

            print(question_number)

            print(raw_question_text)
    
            update_to_database(lapis_question_tag=lapis_question_tag.strip(),cursor=cursor,raw_latex_data=raw_question_text,option_json=json.dumps(options),question_text =question_text,question_number=question_number,correct_answer=correct_answer,base_chapter_tag=base_chapter_tag)
            conn.commit()
            sucessfully_uploaded += 1
        except Exception as e:
            failed_to_upload += 1
    
            try:
                failed_list.append({
                    "lapis_question_tag":lapis_question_tag,
                    "error":str(e),
                    "text":raw_question_text,
                    "traceback":traceback.format_exc()
                })[-1]
            except:
                continue
            continue

    print(f"{len(result_list)} Question were deteceted")
    
    print(f"Sucessfullly Uploaded {sucessfully_uploaded} \n Failed {failed_to_upload}")

    for failed_question in failed_list:
        print(failed_question)
        print()
    conn.close()
    
    data_to_return = {
        "number_of_question_detected":len(result_list),
        "number_of_question_uploaded":sucessfully_uploaded,
        "number_of_question_failed":failed_to_upload,
        "failed_list":failed_list,
    }
    
    return data_to_return

if __name__ == '__main__':
    # for file_name in ['C6S','C7S','C8S']:
    # for file_name in ['C6M','C6S','C7S','C7M','C8S','C8M']:
    for file_name in ['C6S']:
        sync_latex_from_overleaf_to_database(file_name=file_name)
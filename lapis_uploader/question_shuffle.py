try:
    from . import db_con 
except:
    import db_con
  
import random
  
def shuffle_question_by_qpcode(qp_code):
    df = db_con.processQuery(query=f"select question_id from lapis.lapis_correct_options_detail where base_question_paper_code = {qp_code}")
    
    if df.empty:
        raise Exception(f"No Question selected for qp_code {qp_code}")
    
    question_selected_list = df['question_id'].to_list()

    easy_question = [x for x in question_selected_list if str(x).__contains__('B')]
    
    regular_question = [x for x in question_selected_list if str(x).__contains__('DT')]
    
    critical_question = [x for x in question_selected_list if str(x).__contains__('CT')]
    
    final_question_list = []
    
    final_question_list += easy_question
    
    final_question_list += regular_question
     
    final_question_list += critical_question
    
    unique_question_list = list(set(final_question_list))
    
    if len(unique_question_list) != len(final_question_list):
        raise Exception("Question Repeated")
    
    # for i in range(len(final_question_list)):
    #     print(final_question_list[i],question_selected_list[i],(final_question_list[i] ==question_selected_list[i]) )
    # # random.shuffle(question_selected_list)
    
    conn,cursor = db_con.create_connection()
    
    try:
        for index,each_question_id in enumerate(final_question_list):
            query = f"update lapis.lapis_correct_options_detail set base_question_number = {index+1} where base_question_paper_code = {qp_code} and question_id = '{each_question_id}'"
            
            db_con.excute_query_without_commit(query=query,cursor=cursor)
    except Exception as e:
        
        raise Exception(f"Unable to update the database for qp_code {qp_code}")
        
    conn.commit()
    
    return {"message":f"successfully shuffled questions for qp_code {qp_code}"}

if __name__ == '__main__':
    print(shuffle_question_by_qpcode(qp_code=624))
import psycopg2 as py
from lb_tech_handler import db_handler as db

def get_qp_variant(lapis_roll_number):
    
    try:

        query = """SELECT extra_data FROM exam.exam_document_track
                    where roll_number  = %(lapis_roll_number)s """
        Question_paper_variant = db.execute_query_and_return_result(query=query,vars={'lapis_roll_number':lapis_roll_number})

        for qp in Question_paper_variant:
            print(qp[0])
    
    except Exception as e:
        print(f"Error: {e}")


if __name__ == '__main__':
    lapis_roll_number = 'C10001'
    get_qp_variant(lapis_roll_number)
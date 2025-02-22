from pylatex import Document, Section, MiniPage, Tabularx, MultiRow, TikZ, NoEscape, Package
from pylatex.utils import bold
import pandas as pd
import psycopg2 as py
import os
import qrcode
import barcode
from barcode.writer import ImageWriter
import shutil
import json
from minio import Minio
from minio.error import S3Error
from datetime import timedelta
from dotenv import load_dotenv
from lb_tech_handler import db_handler as db
from lapis_uploader.qp_variant_suggestion import get_qp_varaint_by_roll_number

load_dotenv()

class OMRGenerator:
    def __init__(self, school_id):
        """_summary_

        Args:
            school_id (int): School id
        """
        # DB Credentials
        self.LB_DB_HOST_NAME_OR_IP = os.getenv("LB_DB_HOST_NAME_OR_IP")
        self.LB_DB_USER_NAME = os.getenv("LB_DB_USER_NAME")
        self.LB_DB_PASSWORD = os.getenv("LB_DB_PASSWORD")
        self.LB_DB_PORT = os.getenv("LB_DB_PORT")
        self.LB_DB_DATABASE_NAME = os.getenv("LB_DB_DATABASE_NAME")
        
        #LB_DB_DEV
        self.LB_DB_DEV_DATABASE_NAME = os.getenv("LB_DB_DEV_DATABASE_NAME")

        #Minio credentials

        self.Acesskey = os.getenv("accessKey")
        self.Secretkey = os.getenv("secretKey")


        self.school_id = school_id
        
        self.omr_serial_number = {"01": 1001}
        
        self.output_folder = self.create_output_folder()

        self.conn, self.cur = self.connect_db()

        #self.dev_conn, self.dev_cur = self.LB_DB_connection()

    def create_output_folder(self):
        """Create output folder if it doesn't exist."""
        main_folder = os.getcwd()
        output_folder = os.path.join(main_folder, "Omr_Output_folder")
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        return output_folder
    

    def create_mul_folder(self,student):
        """_summary_

        Args:
            student (_type_): pass the data 

        Returns:
            _type_: _description_
        """
        school_folder = os.path.join(self.output_folder,str(student['school_name']))
        class_folder = os.path.join(school_folder,str(student['class']))
        section_folder = os.path.join(class_folder,str(student['section']))

        for folder in [school_folder, class_folder, section_folder]:
            if not os.path.exists(folder):
                os.makedirs(folder)
        
        return section_folder

    def connect_db(self):
        """Connect to the PostgreSQL database."""
        conn = py.connect(
            dbname=self.LB_DB_DATABASE_NAME,
            user=self.LB_DB_USER_NAME,
            password=self.LB_DB_PASSWORD,
            host=self.LB_DB_HOST_NAME_OR_IP,
            port=self.LB_DB_PORT
        )
        cur = conn.cursor()
        return conn, cur

    def fetch_student_data(self,lapis_roll_number):
        """Fetch student data from the database."""
        rows = db.execute_query_and_return_result(f"""SELECT 
    roll_number AS lapis_roll_number,
    student_name,
    school_name,
    qpd.class,
    sd.section_id,
    section,
    planned_exam_date AS exam_date,
    qpd.question_paper_code AS qp_code,
    subject AS subject_name,
    academic_year_id
FROM 
    school_data.student_detail AS stud_d
JOIN 
    school_data.school_section_detail AS sd 
    ON sd.section_id = stud_d.section_id
JOIN 
    school_data.school_academic_detail AS sad 
    ON sad.section_id = sd.section_id
JOIN 
    school_data.school_detail AS ssd 
    ON ssd.school_id = sd.school_id
JOIN 
    exam.exam_detail AS ed 
    ON ed.section_id = sd.section_id
JOIN 
    exam.exam_qp_code AS eqc 
    ON eqc.exam_id = ed.exam_id
JOIN 
    exam.question_paper_detail AS qpd 
    ON qpd.question_paper_code = eqc.question_paper_code
JOIN 
    school_data.subject_detail AS sdt 
    ON sdt.subject_id = sad.subject_id
WHERE 
    ssd.school_id = {self.school_id}
    AND qpd.question_paper_type = 'Base Question paper'
	and roll_number = '{lapis_roll_number}'
ORDER BY stud_d.roll_number ASC """)
        
        return pd.DataFrame(rows, columns=['lapis_roll_number', 'student_name', 'school_name', 'class', 'section_id', 'section', 'exam_date', 'qp_code', 'subject_name', 'academic_year_id'])

   
    
    def insert_data_to_the_DB(self,student):

        try:
            insert_query = """INSERT INTO exam.exam_document_track(
        document_type_id, exam_document_level, roll_number, section_id,class, school_id, status, is_additional_document,is_additional_document_used, created_by)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (exam_document_track_id) 
        DO UPDATE SET 
            document_type_id =EXCLUDED.document_type_id,
            exam_document_level = EXCLUDED.exam_document_level,
            roll_number = EXCLUDED.roll_number,
            section_id = EXCLUDED.section_id,
            class = EXCLUDED.class,
            school_id = EXCLUDED.school_id,
            status = EXCLUDED.status, 
            is_additional_document = EXCLUDED.is_additional_document,
            is_additional_document_used = EXCLUDED.is_additional_document_used,
            created_by = EXCLUDED.created_by
        RETURNING exam_document_track_id
        
        """ 
            track_id = db.execute_query_and_return_result(query=insert_query,vars = (1, 'Student', student['lapis_roll_number'], student['section_id'],
                                                        student['class'], self.school_id, 'Generated', 'False', 'False', 47))
    
            #self.cur.execute(insert_query,(1,'Student',student['lapis_roll_number'],student['section_id'],student['class'],self.school_id,'Generated','False','False',47))
            
            self.exam_doc_track_id = track_id[0][0]
            print(f"Inserted/Updated exam_document_track_id: {self.exam_doc_track_id}")
            
            #self.conn.commit()

        except Exception as e:
            print(f"Error inserting data for student {student['lapis_roll_number']}: {e}")
            self.conn.rollback()  # Rollback transaction in case of error

        #return self.exam_doc_track_id

    def update_some_column_in_DB(self):

        try:
            update_query = """
            UPDATE exam.exam_document_track 
            SET document_name = %s, document_url = %s, extra_data = %s
            WHERE exam_document_track_id = %s                                
            """
            extra_data = { "question_paper_variant": self.code}

            extra_data_json = json.dumps(extra_data)

            #print(f'presigned_url--{self.presigned_url}')

            db.execute_query(query=update_query,vars=(os.path.basename(self.pdf_filename),self.presigned_url,extra_data_json,self.exam_doc_track_id))

            #self.cur.execute(update_query,(os.path.basename(self.pdf_filename),self.presigned_url,extra_data_json,self.exam_doc_track_id))
              
            #self.conn.commit()
            print(f"Updated exam_document_track_id: {self.exam_doc_track_id}")

        except Exception as e:
            print(f"Error updating data for exam_document_track_id {self.exam_doc_track_id}: {e}")
            self.conn.rollback()
           


    def create_latex_document(self, student, omr_type,  error=True):
        """Generate a LaTeX document for the given student or without particulars.

        Args:
            student (dict or None): Student data or None for no particulars.
            omr_type (int): Type for QR code and barcode.
            error (bool, optional): Determines if it's an error document. Defaults to True.
        """
        
        
        #self.serial_number = self.omr_serial_number.get(omr_type, 1001)
        #self.omr_serial_number[omr_type] = self.serial_number + 1

        if isinstance(student, pd.Series):
            student = student.to_dict()

        # Handle the case where student data is not provided
        if student and isinstance(student, dict):
            academic_year = student["academic_year_id"]
            class_level = student["class"]
            section = student['section']
            lapis_roll_number = student['lapis_roll_number']

            # Generate QR code and Barcode data using student data
            qr_data = f"rl-{lapis_roll_number},mq-610{section},sq-661{section}"
            barcode_data = str(self.exam_doc_track_id)

            self.generate_qr_code(qr_data, student["lapis_roll_number"])
            self.generate_barcode(barcode_data, student["lapis_roll_number"] )    


        doc = self.initialize_latex_doc()

        if student and isinstance(student, dict):
            doc = self.add_student_commands(doc, student)
   
        if student and isinstance(student, dict):
            doc = self.add_omr_page_content(doc,student)
        

        if student and isinstance(student, dict):
            doc = self.add_student_particular(doc,student)

        doc = self.add_bubles_content(doc)

        mul_folder = self.create_mul_folder(student) 

        self.pdf_filename = os.path.join(self.output_folder, f'omr_sheet_{student["lapis_roll_number"]}-{self.code}')

        if error:
            doc.append(NoEscape(r'\ddd'))

        doc.generate_pdf(self.pdf_filename, compiler='pdflatex', clean_tex=False)

        
        self.move_the_pdf_to_the_belonging_folder(self.pdf_filename, mul_folder, student)


    def move_the_pdf_to_the_belonging_folder(self,pdf_filename,mul_folder,student):
        """_summary_

        Args:
            pdf_filename (path): sourcepath 
            mul_folder (path): destination path
            student (dict): student data
        """

        try:
            s_path = os.path.join(self.output_folder,f'{pdf_filename}.pdf')
            d_path = os.path.join(mul_folder)
            shutil.move(s_path,d_path) 
            print(f"Moved {pdf_filename} to {mul_folder}")
            try:
                s_path = os.path.join(self.output_folder,f'{pdf_filename}.pdf')
                d_path = os.path.join(mul_folder)
                shutil.move(s_path,d_path) 
                print(f"Moved {pdf_filename} to {mul_folder}")

            except Exception as e:
                print(f'The file failed to move {e}')
        except Exception as e:
            print(f'The file failed to move {e}')


        self.upload_pdf_to_minio(d_path,student)

    

    def upload_pdf_to_minio(self, d_path, student):
        """_summary_

        Args:
            d_path (path): source folder
            student (df): to get require school name ,class ,section

        Returns:
            _type_: _description_
        """
        # MinIO client setup
        client = Minio(
            endpoint="s3.learnbasics.fun",  
            access_key=self.Acesskey,
            secret_key=self.Secretkey,
            secure=True
        )

        bucket_name = "omr" 
        if student is not None:
            main_folder = "generated_omr_sheet/2024-2025/"
            school_folder = f"{main_folder}{student['school_name']}/"
            class_folder = f"{school_folder}{student['class']}/"
            section_folder = f"{class_folder}{student['section']}/"

            # Source file path
            source_file = f'{d_path}/omr_sheet_{student["lapis_roll_number"]}-{self.code}.pdf' 
            minio_pdf_path = f'{section_folder}omr_sheet_{student["lapis_roll_number"]}-{self.code}.pdf' 

        
        try:
            # Open the file and get its size
            with open(source_file, 'rb') as file_data:
                file_stat = os.stat(source_file)
                
                # Upload the PDF to MinIO
                client.put_object(
                    bucket_name=bucket_name,
                    object_name=minio_pdf_path,
                    data=file_data,
                    length=file_stat.st_size,
                    content_type='application/pdf'
                )

            print(f"Successfully uploaded {source_file} to {minio_pdf_path} in MinIO.")


             # Generate a pre-signed URL for the uploaded file (valid for 7 days)
            self.presigned_url = client.get_presigned_url(
                method="GET",
                bucket_name=bucket_name,
                object_name=minio_pdf_path,
                expires=timedelta(days=7)  # URL expiry time (e.g., 7 days)
        )
        
            print(f"Pre-signed URL for {minio_pdf_path}: {self.presigned_url}")

            return self.presigned_url

            
        except S3Error as err:
            print(f"Error uploading {source_file}: {err}")
        except Exception as e:
            print(f"Unexpected error occurred: {e}")

        return client

                

    def generate_qr_code(self, data, roll_number):
        """_summary_

        Args:
            data (_type_): data for the QR
            roll_number (INT): For create a qr uniquely

        Returns:
            _type_: _description_
        """
        qr_filename = os.path.join(self.output_folder, f'qr{roll_number}.png')
        
        qr = qrcode.QRCode(
        version=2,  
        error_correction=qrcode.constants.ERROR_CORRECT_L, 
        box_size=10,  
        border=1, 
    )

        # Add data to the QR code
        qr.add_data(data)
        qr.make(fit=True)

        # Generate the QR code image
        img = qr.make_image(fill='black', back_color='white')

        # Save the QR code image to a file
        img.save(qr_filename)
        return qr_filename

    def generate_barcode(self, data, roll_number):
        """_summary_

        Args:
            data (_type_): data for the Barcode
            roll_number (int): For create a Barcode uniquely

        Returns:
            _type_: _description_
        """
        barcode_filename = os.path.join(self.output_folder, f'barcode{roll_number}-{self.exam_doc_track_id}')
        code = barcode.get('code128', data, writer=ImageWriter())
        code.save(barcode_filename,options={"write_text": False})
        return f'{barcode_filename}.png'  # Assuming barcode saves as PNG

    
    def initialize_latex_doc(self):
        """Initialize a LaTeX document with packages and preamble."""
        geometry_options = {
            "top": "0.5cm",
            "bottom": "1.5cm",
            "left": "1cm",
            "right": "1cm"
        }
        doc = Document(documentclass="article", document_options=["10pt", "a4paper"], geometry_options=geometry_options)
        # Add required packages
        packages = ['tikz', 'subfig', 'fancyhdr', 'amsmath', 'geometry', 'mdframed', 'ragged2e', 'tabularx', 'multirow', 'graphicx', 'xcolor', 'array']
        for pkg in packages:
            doc.packages.append(Package(pkg))

        return doc

    def add_student_commands(self, doc, student):
        """_summary_

        Args:
            doc (_type_): document
            student (df): Dataframe
        """        """"""


        if student and isinstance(student, dict): 
            doc.preamble.append(NoEscape(r'\newcommand{\rowSpacing}{0.69cm}'))  # Custom row spacing
            doc.preamble.append(NoEscape(r'\newcommand{\omrMarker}{../omr_marker.png}'))
            doc.preamble.append(NoEscape(r'\newcommand{\omrMarkerWidth}{1cm}'))
            doc.preamble.append(NoEscape(r'\newcommand{\bubbleSpacing}{0.6}'))
            doc.preamble.append(NoEscape(r'\newcommand{\maxQuestions}{60}'))
            doc.preamble.append(NoEscape(r'\newcommand{\OMRID}{87654321}'))
            doc.preamble.append(NoEscape(r'\newcommand{\QRcodePath}{%s}' %f'qr{student["lapis_roll_number"]}.png'))
            doc.preamble.append(NoEscape(r'\newcommand{\BarcodePath}{%s}' %f'barcode{student["lapis_roll_number"]}-{self.exam_doc_track_id}'))
            doc.preamble.append(NoEscape(rf'\newcommand{{\studentName}}{{{student["student_name"]}}}'))
            doc.preamble.append(NoEscape(rf'\newcommand{{\rollNo}}{{{student["lapis_roll_number"]}-{self.code}}}'))
            doc.preamble.append(NoEscape(rf'\newcommand{{\schoolName}}{{{student["school_name"]}}}'))
            doc.preamble.append(NoEscape(rf'\newcommand{{\sectionClass}}{{{student["class"]}-{student["section"]}}}'))
            doc.preamble.append(NoEscape(rf'\newcommand{{\examDate}}{{{student["exam_date"]}}}'))
            doc.preamble.append(NoEscape(rf'\newcommand{{\QPCode}}{{Math-(610-{student["section"]}) Science-(661-{student["section"]})}}'))
        
        return doc
    
    def add_omr_page_content(self,doc,student):
        """_summary_

        Args:
            doc (_type_): document
            student (df): Dataframe
        """        """"""

        if student and isinstance(student, dict):
            doc.preamble.append(NoEscape(r"""
            \fancypagestyle{bodypage}{
            \renewcommand{\headrulewidth}{0pt}
            \renewcommand{\footrulewidth}{0pt}
            \fancyhead{}
            \fancyfoot[L]{www.learnbasics.fun}
            \fancyfoot[R]{\includegraphics[width=4cm,height=0.5cm]{\BarcodePath}}
            \fancyfoot[C]{\schoolName}
            \renewcommand{\headrulewidth}{0pt}
            \renewcommand{\footrulewidth}{1pt}
        }
        """))
            
        
        doc.append(NoEscape(r"""
\newmdenv[
    topline=true,          % Include a top line
    bottomline=true,       % Include a bottom line
    rightline=true,        % Include a right line
    leftline=true,         % Include a left line
    linewidth=1pt,         % Line width of the border
    innertopmargin=0.2cm,   % Inner top margin inside the frame
    innerbottommargin=0.2cm,% Inner bottom margin inside the frame
    skipabove=0cm,        % Vertical space above the frame
    skipbelow=0cm,        % Vertical space below the frame
]{customframe}

\newmdenv[
    topline=false,          % Include a top line
    bottomline=false,       % Include a bottom line
    rightline=false,        % Include a right line
    leftline=false,         % Include a left line
    linewidth=1pt,         % Line width of the border
    innertopmargin=0cm,   % Inner top margin inside the frame
    innerbottommargin=0cm,% Inner bottom margin inside the frame
    skipabove=0cm,        % Vertical space above the frame
    skipbelow=0cm,        % Vertical space below the frame
]{fullpage}

\newmdenv[
    topline=false,          % Include a top line
    bottomline=false,       % Include a bottom line
    rightline=false,        % Include a right line
    leftline=false,         % Include a left line
    linewidth=1pt,         % Line width of the border
    innertopmargin=0cm,   % Inner top margin inside the frame
    innerbottommargin=0cm,% Inner bottom margin inside the frame
    skipabove=0cm,        % Vertical space above the frame
    skipbelow=0cm,        % Vertical space below the frame
]{subjectpage}

\newmdenv[
    topline=true,          % Include a top line
    bottomline=true,       % Include a bottom line
    rightline=true,        % Include a right line
    leftline=true,         % Include a left line
    linewidth=1pt,         % Line width of the border
    innertopmargin=0.2cm,   % Inner top margin inside the frame
    innerbottommargin=0.2cm,% Inner bottom margin inside the frame
    skipabove=0cm,        % Vertical space above the frame
    skipbelow=0.5cm, 
    innerleftmargin = 0cm% Vertical space below the frame
]{headpage}

\normalsize%
\pagestyle{bodypage}%

\renewcommand{\arraystretch}{1.68}

\begin{fullpage}
\centering{\textbf{\large{Learn Basics's - LaPIS Diagnostic Test}}}

\begin{tikzpicture}[remember picture, overlay]
    % Top-left corner
    \node[anchor=north west, xshift=0.5cm, yshift=0cm] at (current page.north west) {\includegraphics[width=\omrMarkerWidth]{\omrMarker}};
    
    % Top-right corner
    \node[anchor=north east, xshift=-0.5cm, yshift=0cm] at (current page.north east) {\includegraphics[width=\omrMarkerWidth]{\omrMarker}};
    
    % Bottom-left corner
    \node[anchor=south west, xshift=0.5cm, yshift=1cm] at (current page.south west) {\includegraphics[width=\omrMarkerWidth]{\omrMarker}};
    
    % Bottom-right corner
    \node[anchor=south east, xshift=-0.5cm, yshift=1cm] at (current page.south east) {\includegraphics[width=\omrMarkerWidth]{\omrMarker}};
\end{tikzpicture}

\noindent % Prevent indentation for the first minipage
\begin{minipage}{\textwidth}
\begin{headpage}

\noindent

\setlength{\tabcolsep}{0.1cm}"""))
        
        return doc
        
        
    def add_student_particular(self,doc,student):
        """_summary_

        Args:
            doc (_type_): Document
            student (DF): Dataframe
        """        """"""

        if student and isinstance(student, dict):
            doc.append(NoEscape(r"""

\begin{tabular}{
>{\raggedleft}p{0.145\textwidth} 
>{\raggedright}p{0.25\textwidth}
>{\raggedleft}p{0.1\textwidth} 
>{\raggedleft}p{0.15\textwidth}
>{\raggedright}p{0.1\textwidth} 
>{\raggedleft}p{0.15\textwidth}
}
\textbf{Name} : & \multicolumn{2}{l}{\studentName} & \textbf{Roll Number} :& \rollNo  & \multirow{4}{*}{\includegraphics[width=3.6cm]{\QRcodePath}} \tabularnewline
\textbf{School Name} : &  \multicolumn{2}{l}{\schoolName} & \textbf{Class} : & \sectionClass & \tabularnewline
\textbf{Question Paper Code} : & \multicolumn{2}{l}{\QPCode} & \textbf{Exam Date} : & \examDate   & \tabularnewline
\textbf{Student Sign} : & &   \multicolumn{2}{l}{\textbf{Invigilator Sign} :} &  \tabularnewline
\end{tabular}

\hfill  % Lower the image

\end{headpage}
\end{minipage}

\vspace{0.2cm}"""))
        
        return doc
    
        
    def add_bubles_content(self,doc):
        doc.append(NoEscape(r"""

\begin{subjectpage}
    \begin{tabular}{
>{\centering}p{0.40\textwidth} 
>{\centering}p{0.08\textwidth} 
>{\centering}p{0.45\textwidth} 
}
\textbf{\large{Science}} & & \textbf{\large{Math}}  \tabularnewline
\end{tabular}
\end{subjectpage}

\noindent % Prevent indentation for the custom frame
\begin{minipage}{0.47\textwidth}

\begin{customframe}%

\pgfmathsetmacro{\halfMaxQuestions}{\maxQuestions/2}
\pgfmathtruncatemacro{\halfMaxQuestionsPartone}{\halfMaxQuestions+1}
\pgfmathtruncatemacro{\halfMaxQuestionsParttwo}{\halfMaxQuestions+2}

\begin{tikzpicture}[font=\small]
    \foreach \line in {1,2,...,\halfMaxQuestions} {
        \begin{scope}[yshift=-\rowSpacing*\line] % Use the constant for vertical spacing
            \foreach \letter/\position in {A/1, B/2, C/3, D/4,E/5} { 
                \node at (0,0) {\normalsize{\line}};
                \node[draw,circle,inner sep=1pt] at ({\position * \bubbleSpacing},0) {\textcolor{black!30}{\small{\letter}}};
            }
        \end{scope}
    }
\end{tikzpicture}
\hfill
\begin{tikzpicture}[font=\small]
    \foreach \line in {\halfMaxQuestionsPartone,\halfMaxQuestionsParttwo,...,\maxQuestions} {
        \begin{scope}[yshift=-\rowSpacing*\line] % Use the constant for vertical spacing
            \foreach \letter/\position in {A/1, B/2, C/3, D/4,E/5} { 
                \node at (0,0) {\normalsize{\line}};
                \node[draw,circle,inner sep=1pt] at ({\position * \bubbleSpacing},0) {\textcolor{black!30}{\small{\letter}}};
            }
        \end{scope}
    }
\end{tikzpicture}
\end{customframe}
\end{minipage}
\hfill
\begin{minipage}{0.47\textwidth}
\begin{customframe}%

\pgfmathsetmacro{\halfMaxQuestions}{\maxQuestions/2}
\pgfmathtruncatemacro{\halfMaxQuestionsPartone}{\halfMaxQuestions+1}
\pgfmathtruncatemacro{\halfMaxQuestionsParttwo}{\halfMaxQuestions+2}

\begin{tikzpicture}[font=\small]
    \foreach \line in {1,2,...,\halfMaxQuestions} {
        \begin{scope}[yshift=-\rowSpacing*\line] % Use the constant for vertical spacing
            \foreach \letter/\position in {A/1, B/2, C/3, D/4,E/5} { 
                \node at (0,0) {\normalsize{\line}};
                \node[draw,circle,inner sep=1pt] at ({\position * \bubbleSpacing},0) {\textcolor{black!30}{\small{\letter}}};
            }
        \end{scope}
    }
\end{tikzpicture}
\hfill
\begin{tikzpicture}[font=\small]
    \foreach \line in {\halfMaxQuestionsPartone,\halfMaxQuestionsParttwo,...,\maxQuestions} {
        \begin{scope}[yshift=-\rowSpacing*\line] % Use the constant for vertical spacing
            \foreach \letter/\position in {A/1, B/2, C/3, D/4, E/5} { 
                \node at (0,0) {\normalsize{\line}};
                \node[draw,circle,inner sep=1pt] at ({\position * \bubbleSpacing},0) {\textcolor{black!30}{\small{\letter}}};
            }
        \end{scope}
    }
\end{tikzpicture}
\end{customframe}
\end{minipage}
\end{fullpage}

\vspace{0.40cm}

\centering{Note: Do not alter the Markers on the OMR as this will cause negative mark.}

"""))
        
        return doc
        

    def generate_omr_sheets(self,omr_type,lapis_roll_number):
        """_summary_

        Args:
            omr_type (int): _description_
        """
        try:
            
            df = self.fetch_student_data(lapis_roll_number)
            for idx,student in df.iterrows():
                
                self.code = (get_qp_varaint_by_roll_number(student['lapis_roll_number']))
                
                print('Processing with the',student['lapis_roll_number'],'rollnumber..')

                print(self.code)
                
                try:
                    self.create_latex_document(student, omr_type,error=True)
                    
                except Exception as e:
                    print("Regenrating")
                    self.insert_data_to_the_DB(student)
                    #print('Finished inserting the pdf data')
                    self.create_latex_document(student, omr_type,error=False)
                    print('-------------------')
                    self.update_some_column_in_DB()
                
        except Exception as e:
            print(f"Error generating on OMR sheets: {e}")

        finally:
            # Close the cursor if it's still open
            if self.cur:
                self.cur.close()
                print("Database cursor closed.")

            # Close the connection if it's still open
            if self.conn:
                self.conn.close()
                print("Database connection closed.")


if __name__ == "__main__":

    omr_generator = OMRGenerator(school_id=110)
    
    omr_generator.generate_omr_sheets(omr_type="01",lapis_roll_number='D110058')

from datetime import datetime
import pickle
import json
from pylatex import Document,PageStyle, Head, Foot
from pylatex.utils import NoEscape
import pylatex as pl
from pylatex.package import Package
from pylatex import UnsafeCommand,Command,MiniPage
import os
import re
import pandas as pd
import pandas as pd
import random
import psycopg2 as py
import barcode
from barcode.writer import ImageWriter
from qp_variant_suggestion import get_qp_varaint_by_roll_number
from lb_tech_handler import db_handler as db
import requests

try:
  from .mino_handler import create_client
  from .db_con import processQuery,create_connection,excute_query_without_commit,excute_query_and_return_result
except ImportError:
  from mino_handler import create_client
  from db_con import processQuery,create_connection,excute_query_without_commit,excute_query_and_return_result


class QuestionPaper:
    def __init__(self,qp_code,school_id, section_id ,is_final=True) -> None:
      """_summary_

      Args:
          qp_code (int): Question paper code
          school_id (int): _description_
          section_id (int): _description_
          is_final (bool, optional): _description_. Defaults to True.

      Raises:
          Exception: _description_
      """      """"""

       #DB Credentials 
      self.LB_DB_HOST_NAME_OR_IP = os.getenv("LB_DB_HOST_NAME_OR_IP")
      self.LB_DB_USER_NAME = os.getenv("LB_DB_USER_NAME")
      self.LB_DB_PASSWORD = os.getenv("LB_DB_PASSWORD")
      self.LB_DB_PORT = os.getenv("LB_DB_PORT")
      self.LB_DB_DATABASE_NAME = os.getenv("LB_DB_DATABASE_NAME")   

      #Db Credentails LB_DB_DEV
      self.LB_DB_DEV_DATABASE_NAME = os.getenv("LB_DB_DEV_DATABASE_NAME")

      self.conn,self.cur  = self.conn_to_lb_db()

      self.is_final = is_final

      self.qp_code = qp_code
      
      self.school_id = school_id
      
      self.section_id = section_id

      bridge_course_query = f"""SELECT ssd.school_name,class,subject_name,ls.subject_id,lei.is_interest_group,track FROM lapis.lapis_exam_info lei 
      join lapis.interest_group lig on lig.interest_group_id = lei.interest_group_id
      join lapis.lapis_subject ls on ls.subject_id = lei.subject_id 
      join lapis.school_detail ssd on ssd.school_id = lig.school_id
      where qp_code = {qp_code} and lig.school_id = {school_id}"""
      
      if self.section_id:
         query=f"""select school_name,roll_number,sd.class,subject,sad.subject_id,sad.is_interest_group

		from exam.exam_detail ed 

        join school_data.school_section_detail sd on sd.section_id = ed.section_id

		join school_data.school_academic_detail sad on sad.section_id = sd.section_id

        join school_data.school_detail ssd on ssd.school_id = sd.school_id

		join school_data.subject_detail sub_d on sub_d.subject_id = sad.subject_id

        join school_data.student_detail sdt on sdt.section_id = ed.section_id

		JOIN exam.exam_qp_code eqc  ON eqc.exam_id = ed.exam_id

		JOIN exam.question_paper_detail qpd ON qpd.question_paper_code = eqc.question_paper_code

        where qpd.question_paper_code  = '{qp_code}' and ssd.school_id = {school_id} and sd.section_id ={section_id} and sub_d.subject_id =101
 
        
        """
      else:
         query=f"""select school_name,sd.class,subject,sad.subject_id,sad.is_interest_group

		from exam.exam_detail ed 

        join school_data.school_section_detail sd on sd.section_id = ed.section_id

		join school_data.school_academic_detail sad on sad.section_id = sd.section_id

        join school_data.school_detail ssd on ssd.school_id = sd.school_id

		join school_data.subject_detail sub_d on sub_d.subject_id = sad.subject_id

        join school_data.student_detail sdt on sdt.section_id = ed.section_id

		JOIN exam.exam_qp_code eqc  ON eqc.exam_id = ed.exam_id

		JOIN exam.question_paper_detail qpd ON qpd.question_paper_code = eqc.question_paper_code

        where qpd.question_paper_code  = '{qp_code}' and ssd.school_id = {school_id} and sub_d.subject_id =101
         """

      data_for_qp_from_data_base = excute_query_and_return_result(query=query)
      if len(data_for_qp_from_data_base) < 1:
        data_for_qp_from_data_base = excute_query_and_return_result(query=bridge_course_query)
        if len(data_for_qp_from_data_base) < 1:
          raise Exception(f"No Data found for qp_code {qp_code} in database")
      
      if self.section_id:
        self.school_name,self.lapis_roll_number,self.std,self.subject,self.subject_id,self.is_interest_groups = data_for_qp_from_data_base[0]
      
      else:
        self.school_name,self.std,self.subject,self.subject_id,self.is_interest_groups = data_for_qp_from_data_base[0]

      self.folder_name = f"overleaf_files/question_papers/"
   
      print(self.school_name,self.std,self.subject,self.subject_id,self.is_interest_groups)
      
      #print(pd.DataFrame(data_for_qp_from_data_base))


    def get_lib(self):
        self.doc.packages.append(Package('draftwatermark'))

        self.doc.preamble.append(NoEscape(r'''
        \usepackage{geometry}
        \usepackage{graphicx}
        \usepackage{multicol}
        \usepackage{multirow}
        \usepackage{tikz}
        \usepackage{tabularx}
        \usepackage{float}
        \usepackage{array,booktabs,ragged2e}
        \usepackage{amsmath}
        \usepackage{siunitx}
        \usepackage{physics}
        \usepackage{xcolor}
        \usepackage{wasysym}
        \usepackage{ulem} %tallymarks
        \usepackage{xkeyval}
        
        \usepackage{geometry}
\usepackage{graphicx}
\usepackage{tikz}
\usepackage{tabularx}
\usepackage{multicol}
\usepackage{multirow}
\usepackage{array,booktabs,ragged2e}
\usepackage{amsmath}
\usepackage{xkeyval}
\usepackage{float}
\usepackage{physics}
\usepackage{tfrupee}
\usepackage{amssymb}
\usepackage{gensymb}
        '''))

    def add_custom_commands(self):
        self.doc.preamble.append(NoEscape(r'''
                                          
                                          

\makeatletter
%-----------------------------------------------------------
             % text bottom 4 option in 1 row
%-----------------------------------------------------------

 \define@key{mcqtextbottomFourOne}{questionnumber}{\def\mcqtextbottomFourOnequestionnumber{#1}} 
 \define@key{mcqtextbottomFourOne}{questionTag}{\def\mcqtextbottomFourOnequestionTag{#1}}
\define@key{mcqtextbottomFourOne}{questiontext}{\def\mcqtextbottomFourOnequestiontext{#1}}
\define@key{mcqtextbottomFourOne}{optionA}{\def\mcqtextbottomFourOneoptionA{#1}}
\define@key{mcqtextbottomFourOne}{optionB}{\def\mcqtextbottomFourOneoptionB{#1}}
\define@key{mcqtextbottomFourOne}{optionC}{\def\mcqtextbottomFourOneoptionC{#1}}
\define@key{mcqtextbottomFourOne}{optionD}{\def\mcqtextbottomFourOneoptionD{#1}}
\define@key{mcqtextbottomFourOne}{correctoption}{\def\mcqtextbottomFourOnecorrectoption{#1}}


\newcommand{\mcqtextbottomFourOne}[1]{%
  \setkeys{mcqtextbottomFourOne}{#1}%
  \vspace{1.5mm}
 
  \vspace{\baselineskip}
  \begin{raggedright}
    \textbf{Question \mcqtextbottomFourOnequestionnumber:} \mcqtextbottomFourOnequestiontext\\
    \medskip
      (a) \medskip \mcqtextbottomFourOneoptionA\\
      (b) \medskip \mcqtextbottomFourOneoptionB\\      
      (c) \medskip \mcqtextbottomFourOneoptionC\\
      (d) \medskip \mcqtextbottomFourOneoptionD\\
   \end{raggedright}
}

%-----------------------------------------------------------

%-----------------------------------------------------------

% DEFINE KEY FOR mcqtextbottomOneFour %

\define@key{mcqtextbottomOneFour}{questionnumber}{\def\mcqtextbottomOneFourquestionnumber{#1}} 
\define@key{mcqtextbottomOneFour}{questiontext}{\def\mcqtextbottomOneFourquestiontext{#1}}
\define@key{mcqtextbottomOneFour}{optionA}{\def\mcqtextbottomOneFouroptionA{#1}}
\define@key{mcqtextbottomOneFour}{optionB}{\def\mcqtextbottomOneFouroptionB{#1}}
\define@key{mcqtextbottomOneFour}{optionC}{\def\mcqtextbottomOneFouroptionC{#1}}
\define@key{mcqtextbottomOneFour}{optionD}{\def\mcqtextbottomOneFouroptionD{#1}}
\define@key{mcqtextbottomOneFour}{questionTag}{\def\mcqtextbottomOneFourquestionTag{#1}} 
\define@key{mcqtextbottomOneFour}{correctoption}{\def\mcqtextbottomOneFourcorrectoption{#1}}

% COMMAND FOR mcqtextbottomOneFour %

\newcommand{\mcqtextbottomOneFour}[1]{%
  \setkeys{mcqtextbottomOneFour}{#1}%
  \vspace{1.5mm}
  \vspace{\baselineskip}
  \begin{raggedright}
    \textbf{Question \mcqtextbottomOneFourquestionnumber:} \mcqtextbottomOneFourquestiontext
    \begin{multicols}{4}
      (a) \medskip \mcqtextbottomOneFouroptionA\\
      (b) \medskip \mcqtextbottomOneFouroptionB\\      
      (c) \medskip \mcqtextbottomOneFouroptionC\\
      (d) \medskip \mcqtextbottomOneFouroptionD\\
    \end{multicols}
  \end{raggedright}
}

%-----------------------------------------------------------

%-----------------------------------------------------------

% Define key FOR mcqtextbottomOneTwo 

\define@key{mcqtextbottomOneTwo}{questionnumber}{\def\mcqtextbottomOneTwoquestion{#1}}
\define@key{mcqtextbottomOneTwo}{questiontext}{\def\mcqtextbottomOneTwoquestiontext{#1}}
\define@key{mcqtextbottomOneTwo}{optionA}{\def\mcqtextbottomOneTwooptionA{#1}}
\define@key{mcqtextbottomOneTwo}{optionB}{\def\mcqtextbottomOneTwooptionB{#1}}
\define@key{mcqtextbottomOneTwo}{questionTag}{\def\mcqtextbottomOneTwoquestionTag{#1}}
\define@key{mcqtextbottomOneTwo}{correctoption}{\def\mcqtextbottomOneTwocorrectoption{#1}}

% COMMAND FOR mcqtextbottomOneTwo %

\newcommand{\mcqtextbottomOneTwo}[1]{%
  \setkeys{mcqtextbottomOneTwo}{#1}%
  \vspace{1.5mm}

  \vspace{\baselineskip}
  \begin{raggedright}
    \textbf{Question \mcqtextbottomOneTwoquestion:} \mcqtextbottomOneTwoquestiontext\\
    \begin{multicols}{2}
      (a) \medskip \mcqtextbottomOneTwooptionA\\
      (b) \medskip \mcqtextbottomOneTwooptionB\\
    \end{multicols}
  \end{raggedright}
}

%-----------------------------------------------------------

%-----------------------------------------------------------

% Define key FOR mcqtextbottomTwoTwo

\define@key{mcqtextbottomTwoTwo}{questionnumber}{\def\mcqtextbottomTwoTwoquestion{#1}}
\define@key{mcqtextbottomTwoTwo}{questiontext}{\def\mcqtextbottomTwoTwoquestiontext{#1}}
\define@key{mcqtextbottomTwoTwo}{optionA}{\def\mcqtextbottomTwoTwooptionA{#1}}
\define@key{mcqtextbottomTwoTwo}{optionB}{\def\mcqtextbottomTwoTwooptionB{#1}}
\define@key{mcqtextbottomTwoTwo}{optionC}{\def\mcqtextbottomTwoTwooptionC{#1}}
\define@key{mcqtextbottomTwoTwo}{optionD}{\def\mcqtextbottomTwoTwooptionD{#1}}
\define@key{mcqtextbottomTwoTwo}{questionTag}{\def\mcqtextbottomTwoTwoquestionTag{#1}}
\define@key{mcqtextbottomTwoTwo}{correctoption}{\def\mcqtextbottomTwoTwocorrectoption{#1}}

% COMMAND FOR mcqtextbottomTwoTwo %

\newcommand{\mcqtextbottomTwoTwo}[1]{%
  \setkeys{mcqtextbottomTwoTwo}{#1}%
  \vspace{1.5mm}
  
  \vspace{\baselineskip}
  \begin{raggedright}
    \textbf{Question \mcqtextbottomTwoTwoquestion:} \mcqtextbottomTwoTwoquestiontext\\
    \begin{multicols}{2}
      (a) \medskip \mcqtextbottomTwoTwooptionA\\
      (c) \medskip \mcqtextbottomTwoTwooptionC\\
      \columnbreak
      (b) \medskip \mcqtextbottomTwoTwooptionB\\
      (d) \medskip \mcqtextbottomTwoTwooptionD\\
    \end{multicols}
  \end{raggedright}
}

%-----------------------------------------------------------

%-----------------------------------------------------------

% Define key FOR mcqtextsideFourOne


\define@key{mcqtextsideFourOne}{questionnumber}{\def\mcqtextsideFourOnequestionnumber{#1}} 
\define@key{mcqtextsideFourOne}{questionTag}{\def\mcqtextsideFourOnequestionTag{#1}} 
\define@key{mcqtextsideFourOne}{questiontext}{\def\mcqtextsideFourOnequestiontext{#1}}
\define@key{mcqtextsideFourOne}{optionA}{\def\mcqtextsideFourOneoptionA{#1}}
\define@key{mcqtextsideFourOne}{optionB}{\def\mcqtextsideFourOneoptionB{#1}}
\define@key{mcqtextsideFourOne}{optionC}{\def\mcqtextsideFourOneoptionC{#1}}
\define@key{mcqtextsideFourOne}{optionD}{\def\mcqtextsideFourOneoptionD{#1}}
\define@key{mcqtextsideFourOne}{correctoption}{\def\mcqtextsideFourOnecorrectoption{#1}}
\define@key{mcqtextsideFourOne}{leftmini}{\def\mcqtextsideFourOneleftmini{#1}}
\define@key{mcqtextsideFourOne}{rightmini}{\def\mcqtextsideFourOnerightmini{#1}}

% COMMAND FOR mcqtextsideFourOne

\newcommand{\mcqtextsideFourOne}[1]{%
  \setkeys{mcqtextsideFourOne}{#1}%
  \vspace{1mm}

  \vspace{\baselineskip}
  \begin{raggedright}
  \begin{minipage}[t]{\mcqtextsideFourOneleftmini\linewidth}
    \textbf{Question \mcqtextsideFourOnequestionnumber:} \mcqtextsideFourOnequestiontext\\
  \end{minipage}\hfill
  \begin{minipage}[t]{\mcqtextsideFourOnerightmini\linewidth}
        (a) \medskip \mcqtextsideFourOneoptionA\\
        (b) \medskip \mcqtextsideFourOneoptionB\\
        (c) \medskip \mcqtextsideFourOneoptionC\\
        (d) \medskip \mcqtextsideFourOneoptionD\\
     \end{minipage}
   \end{raggedright}
}

%-----------------------------------------------------------

%-----------------------------------------------------------

% Define key FOR mcqimgbottomOneFour

\define@key{mcqimgbottomOneFour}{questionnumber}{\def\mcqimgbottomOneFourquestionnumber{#1}}
\define@key{mcqimgbottomOneFour}{questionTag}{\def\mcqimgbottomOneFourquestionTag{#1}}
\define@key{mcqimgbottomOneFour}{questiontext}{\def\mcqimgbottomOneFourquestiontext{#1}}
\define@key{mcqimgbottomOneFour}{optionA}{\def\mcqimgbottomOneFouroptionA{#1}}
\define@key{mcqimgbottomOneFour}{optionB}{\def\mcqimgbottomOneFouroptionB{#1}}
\define@key{mcqimgbottomOneFour}{optionC}{\def\mcqimgbottomOneFouroptionC{#1}}
\define@key{mcqimgbottomOneFour}{optionD}{\def\mcqimgbottomOneFouroptionD{#1}}
\define@key{mcqimgbottomOneFour}{correctoption}{\def\mcqimgbottomOneFourcorrectoption{#1}}

% COMMAND FOR mcqimgbottomOneFour %

\newcommand{\mcqimgbottomOneFour}[1]{%
  \setkeys{mcqimgbottomOneFour}{#1}%
  \vspace{1.5mm}

  \vspace{\baselineskip}
  \begin{raggedright}
    \textbf{Question \mcqimgbottomOneFourquestionnumber:} \mcqimgbottomOneFourquestiontext \\
    \begin{multicols}{4}
      (a) \includegraphics[width=2.5cm, height=2cm]{\mcqimgbottomOneFouroptionA}  \\
      (b) \includegraphics[width=2.5 cm, height=2cm]{\mcqimgbottomOneFouroptionB}   \\
      (c) \includegraphics[width=2.5cm, height=2cm]{\mcqimgbottomOneFouroptionC}  \\
      (d) \includegraphics[width=2.5cm, height=2cm]{\mcqimgbottomOneFouroptionD} 
    \end{multicols}
  \end{raggedright}
}

%-----------------------------------------------------------

%-----------------------------------------------------------

% Define key FOR mcqimgdbottomOneFour

\define@key{mcqimgdbottomOneFour}{questionnumber}{\def\mcqimgdbottomOneFourquestionnumber{#1}}
\define@key{mcqimgdbottomOneFour}{questionTag}{\def\mcqimgdbottomOneFourquestionTag{#1}}
\define@key{mcqimgdbottomOneFour}{questiontext}{\def\mcqimgdbottomOneFourquestiontext{#1}}
\define@key{mcqimgdbottomOneFour}{optionA}{\def\mcqimgdbottomOneFouroptionA{#1}}
\define@key{mcqimgdbottomOneFour}{optionAtext}{\def\mcqimgdbottomOneFouroptionAtext{#1}}
\define@key{mcqimgdbottomOneFour}{optionB}{\def\mcqimgdbottomOneFouroptionB{#1}}
\define@key{mcqimgdbottomOneFour}{optionBtext}{\def\mcqimgdbottomOneFouroptionBtext{#1}}
\define@key{mcqimgdbottomOneFour}{optionC}{\def\mcqimgdbottomOneFouroptionC{#1}}
\define@key{mcqimgdbottomOneFour}{optionCtext}{\def\mcqimgdbottomOneFouroptionCtext{#1}}
\define@key{mcqimgdbottomOneFour}{optionD}{\def\mcqimgdbottomOneFouroptionD{#1}}
\define@key{mcqimgdbottomOneFour}{optionDtext}{\def\mcqimgdbottomOneFouroptionDtext{#1}}
\define@key{mcqimgdbottomOneFour}{correctoption}{\def\mcqimgdbottomOneFourcorrectoption{#1}}

% COMMAND FOR mcqimgdbottomOneFour %

\newcommand{\mcqimgdbottomOneFour}[1]{%
  \setkeys{mcqimgdbottomOneFour}{#1}%
  \vspace{1.5mm}

  \vspace{\baselineskip}
  \begin{raggedright}
    \textbf{Question \mcqimgdbottomOneFourquestionnumber:} \mcqimgdbottomOneFourquestiontext \\
    \begin{multicols}{4}
       \includegraphics[width=2.5cm, height=2cm]{\mcqimgdbottomOneFouroptionA} \\*
      (a) \mcqimgdbottomOneFouroptionAtext \\

       \includegraphics[width=2.5cm, height=2cm]{\mcqimgdbottomOneFouroptionB} \\*      
      (b) \mcqimgdbottomOneFouroptionBtext \\

       \includegraphics[width=2.5cm, height=2cm]{\mcqimgdbottomOneFouroptionC} \\*
      (c) \mcqimgdbottomOneFouroptionCtext \\

       \includegraphics[width=2.5cm, height=2cm]{\mcqimgdbottomOneFouroptionD} \\*
      (d) \mcqimgdbottomOneFouroptionDtext
    \end{multicols}
  \end{raggedright}
  }

%-----------------------------------------------------------

%-----------------------------------------------------------

% Define key FOR mcqimgleftFourOne

\define@key{mcqimgleftFourOne}{questionnumber}{\def\mcqimgleftFourOnequestionnumber{#1}} 
\define@key{mcqimgleftFourOne}{questiontext}{\def\mcqimgleftFourOnequestiontext{#1}}
\define@key{mcqimgleftFourOne}{imgtabletikz}{\def\mcqimgtabletikz{#1}}
\define@key{mcqimgleftFourOne}{optionA}{\def\mcqimgleftFourOneoptionA{#1}}
\define@key{mcqimgleftFourOne}{optionB}{\def\mcqimgleftFourOneoptionB{#1}}
\define@key{mcqimgleftFourOne}{optionC}{\def\mcqimgleftFourOneoptionC{#1}}
\define@key{mcqimgleftFourOne}{optionD}{\def\mcqimgleftFourOneoptionD{#1}}
\define@key{mcqimgleftFourOne}{questionTag}{\def\mcqimgleftFourOnequestionTag{#1}} 
\define@key{mcqimgleftFourOne}{correctoption}{\def\mcqimgleftFourOnecorrectoption{#1}}
\define@key{mcqimgleftFourOne}{leftmini}{\def\mcqimgleftFourOneleftmini{#1}}
\define@key{mcqimgleftFourOne}{rightmini}{\def\mcqimgleftFourOnerightmini{#1}}

% COMMAND FOR mcqimgleftFourOne 

\newcommand{\mcqimgleftFourOne}[1]{%
  \setkeys{mcqimgleftFourOne}{#1}%
  \vspace{1.5mm}

  \vspace{\baselineskip}
  \begin{raggedright}
    \textbf{Question \mcqimgleftFourOnequestionnumber:} \mcqimgleftFourOnequestiontext\\
    \medskip
  \end{raggedright}
  \begin{minipage}[]{\mcqimgleftFourOneleftmini\linewidth}
  \Centering
    \mcqimgtabletikz 
  \end{minipage}\hfill
  \begin{minipage}[]{\mcqimgleftFourOnerightmini\linewidth}
      (a) \medskip \mcqimgleftFourOneoptionA\\
      (b) \medskip \mcqimgleftFourOneoptionB\\
      (c) \medskip \mcqimgleftFourOneoptionC\\
      (d) \medskip \mcqimgleftFourOneoptionD\\  
   \end{minipage}
   \vspace{2mm}}

%-----------------------------------------------------------

%-----------------------------------------------------------

% DEFINE KEY FOR mcqfourimg %

\define@key{mcqimgsideFourOne}{questionnumber} {\def\mcqimgsideFourOnequestionnumber{#1} }
\define@key{mcqimgsideFourOne}{questiontext}{\def\mcqimgsideFourOnequestiontext{#1}}
\define@key{mcqimgsideFourOne}{imgwidth}{\def\mcqimgsideFourOnewidth{#1}}
\define@key{mcqimgsideFourOne}{imgheight}{\def\mcqimgsideFourOneheight{#1}}
\define@key{mcqimgsideFourOne}{img}{\def\mcqimgsideFourOne{#1}}
\define@key{mcqimgsideFourOne}{optionA}{\def\mcqimgsideFourOneoptionA{#1}}
\define@key{mcqimgsideFourOne}{optionB}{\def\mcqimgsideFourOneoptionB{#1}}
\define@key{mcqimgsideFourOne}{optionC}{\def\mcqimgsideFourOneoptionC{#1}}
\define@key{mcqimgsideFourOne}{optionD}{\def\mcqimgsideFourOneoptionD{#1}}
\define@key{mcqimgsideFourOne}{questionTag}{\def\mcqimgsideFourOnequestionTag{#1}} 
\define@key{mcqimgsideFourOne}{correctoption}{\def\mcqimgsideFourOnecorrectoption{#1}}
\define@key{mcqimgsideFourOne}{leftmini}{\def\mcqimgsideFourOneleftmini{#1}}
\define@key{mcqimgsideFourOne}{rightmini}{\def\mcqimgsideFourOnerightmini{#1}}

\newcommand{\mcqimgsideFourOne}[1]{
  \setkeys{mcqimgsideFourOne}{#1}
  \vspace{1.5mm}

  \vspace{\baselineskip}
  \begin{raggedright}
    \begin{minipage}[]{\mcqimgsideFourOneleftmini\textwidth}
    \textbf{Question \mcqimgsideFourOnequestionnumber:} \mcqimgsideFourOnequestiontext
    \vspace{2mm} \\
        (a) \medskip \mcqimgsideFourOneoptionA \\
        (b) \medskip \mcqimgsideFourOneoptionB \\
        (c) \medskip \mcqimgsideFourOneoptionC \\
        (d) \medskip \mcqimgsideFourOneoptionD \\
    \end{minipage}
    \begin{minipage}[]{\mcqimgsideFourOnerightmini\textwidth}
    \includegraphics[width=\mcqimgsideFourOnewidth, height=\mcqimgsideFourOneheight]{\mcqimgsideFourOne}
    \end{minipage}
   \end{raggedright}
         }

%-----------------------------------------------------------
%               MCQ without Option
%-----------------------------------------------------------


\define@key{mcqdescriptive}{questionnumber}{\def\mcqdescriptivequestionnumber{#1}} 
\define@key{mcqdescriptive}{questionTag}{\def\mcqdescriptivequestionTag{#1}} 
\define@key{mcqdescriptive}{questiontext}{\def\mcqdescriptivequestiontext{#1}}
\define@key{mcqdescriptive}{correctoption}{\def\mcqdescriptivecorrectoption{#1}}

\newcommand{\mcqdescriptive}[1]{%
  \setkeys{mcqdescriptive}{#1}%
  \vspace{1.5mm}

  \vspace{\baselineskip}
\begin{raggedright}
    \textbf{Question \mcqdescriptivequestionnumber:} \mcqdescriptivequestiontext
\end{raggedright} 
}


%-----------------------------------------------------------
%                        TABLE
%-----------------------------------------------------------
\newcolumntype{R}[1]{>{\Centering\arraybackslash}p{#1}}

\makeatother
        '''))

    def add_header_bridge_course(self):
      
        
      self.doc.append(NoEscape(r'''
                               
        \begin{minipage}{0.95\textwidth}%
        \vspace{0.7cm}
        \begin{center}
            {\Large \textbf{Bridge Course Completion Test}}\\\vspace{1mm}
   
        
          \renewcommand{\arraystretch}{1.5}
        \begin{tabular}{| >{\centering}p{0.3\textwidth} |  >{\centering}p{0.2\textwidth} |  >{\centering}p{0.4\textwidth} |}
            \hline 
            '''+
             r' \textbf{Class '+str(self.std)+' to '+ str(self.std + 1) + ' - ' +self.subject +r'} &  \textbf{Track '+ self.track + r'} & \textbf{Question Paper Code : '+ (str(self.qp_code)) +r'''} \tabularnewline
            \hline
        \end{tabular}
        \vspace{0.3cm}
        \end{center}
        
        \begin{raggedleft}
        \end{raggedleft} 
        
        \begin{raggedright} 
        \textbf{ Roll Number : \rule{3cm}{1pt}} \hfill \textbf{Duration : 1 Hr, Marks : '''+str(self.total_marks)+r'''} \\
        \end{raggedright} 
        \end{minipage}%

'''))

    def add_header_regular(self):
        
        if self.section_id:

          self.doc.preamble.append(NoEscape(r'''
        %------------------------------------------------------------
        %                    HEADER INFORMATION
        %------------------------------------------------------------
        \newcommand{\hdr}[7]{
        \renewcommand{\arraystretch}{1.5}
\begin{center}
    {\Large \textbf{#1}}\\\vspace{0mm}
  
\end{center}

\begin{tabular}{
>{\RaggedRight}p{0.3\textwidth}
>{\centering}p{0.18\textwidth}
>{\RaggedLeft}p{0.4\textwidth}
}
        
\textbf{Class : } #6 &  & \textbf{Roll Number : E#7'''+str(self.lapis_roll_number)+r'''}  \tabularnewline

\textbf{Subject : } #2 &  & \textbf{Question Paper Code : }#3  \tabularnewline

\textbf{Exam Duration : }#4 &  & #5  \tabularnewline

\end{tabular}

\vspace{1mm}
} '''))
        else:
           self.doc.preamble.append(NoEscape(r'''
        %------------------------------------------------------------
        %                    HEADER INFORMATION
        %------------------------------------------------------------
        \newcommand{\hdr}[7]{
        \renewcommand{\arraystretch}{1.5}
\begin{center}
    {\Large \textbf{#1}}\\\vspace{0mm}
  
\end{center}

\begin{tabular}{
>{\RaggedRight}p{0.3\textwidth}
>{\centering}p{0.18\textwidth}
>{\RaggedLeft}p{0.4\textwidth}
}
        
\textbf{Class : } #6 &  & \textbf{Roll Number : E#7}\rule{70pt}{0.5pt}  \tabularnewline

\textbf{Subject : } #2 &  & \textbf{Question Paper Code : }#3  \tabularnewline

\textbf{Exam Duration : }#4 &  & #5  \tabularnewline

\end{tabular}

\vspace{1mm}
} '''))
           

        

        # self.doc.append(NoEscape(r'\hdr{LaPIS Diagnostic Test - Class '+str(self.std)+r'}{From Learn Basics - www.learnbasics.fun}{'+self.subject.capitalize()+r' - Question Paper Code - '+str(self.qp_code)+r'}{Roll Number }{1 hr 30 min}{Total Questions - '+str(self.total_marks)+r' ,Total Marks - '+str(self.total_marks)+r'}{Swami School - Enhanced by Learn Basics}'))
        #self.doc.append(NoEscape(r'\hdr{LaPIS Diagnostic Test - '+self.school_name+r'}{Question Paper Code - '+str(self.qp_code)+r" - "+(self.code)+r'}{Roll Number }{1 hr 30 min}{Total Questions - '+str(self.total_marks)+r' ,Total Marks - '+str(self.total_marks)+r'}{Class - '+str(self.std)+" - "+self.subject.capitalize()+r'}'))
        self.doc.append(NoEscape(r'\hdr{' + 'LaPIS Diagnostic Test - ' + self.school_name +  r'}{' + self.subject.capitalize() +  r'}{' + str(self.qp_code) + r' - ' + self.code +  r'}{1 hr 30 min}{Total Questions - ' + str(self.total_marks) +  r', Total Marks - ' + str(self.total_marks) +  r'}{' + str(self.std) +  r'}{'+ str(self.school_id) +  r'}'))    
    
    def add_instuctions(self):
        self.doc.change_document_style("emptypage")
        # if not self.is_interest_groups:

        if not self.subject == 'Science':
            if self.is_interest_groups:
              self.doc.append(NoEscape(r'''
            \vspace{0.3cm}
            
            \begin{raggedright}                   
                      
            \rule{\textwidth}{1pt}
            \vspace{0.1cm}
            \textbf{       \vspace{1mm}    \large INSTRUCTIONS}
            \end{raggedright}
                                    
            \begin{enumerate}
            \item Why there are \textbf{5 options} in the OMR Sheet?
            \begin{itemize}
            \item We appreciate your genuineness, if you don't know the answer you can always mark \textbf{Option E}.
            \item Option E, represents you \textbf{will learn that concept later.}
            \item The objective of this Bridge Course Completion test is to help you identify and bridge the unknown concepts not to evaluate how much you're scoring.
            \end{itemize}
            \item Where can I find the \textbf{Roll Number}?
            \begin{itemize}
            \item Roll number is a \textbf{6 - Digit number} in your \textbf{Attendance Sheet}.
            \end{itemize}
            \item How to fill the \textbf{question paper code}?
            \begin{itemize}
            \item Question paper code is a \textbf{3 - Digit number} in the \textbf{top of this page}.
            \end{itemize}
            \item General Instructions
            \begin{itemize}
            \item The question paper consists of \textbf{'''+str(self.total_marks)+r''' Questions}.
            \item Each question carries \textbf{1 mark}.
            \item Total duration of the test is \textbf{1 hour}.
            \item All questions in the booklet are \textbf{objective type}.
            \item For each question, five options are provided whereas \textbf{option E,} indicates Yet to learn / I will learn later.
            \end{itemize}
            \end{enumerate}
            \rule{\textwidth}{1pt}
            '''))
              
            else:
              
              self.doc.append(NoEscape(r'''
            \rule{\textwidth}{1pt}
            \begin{raggedright}                   
                      
            \textbf{       \vspace{1mm}    \large INSTRUCTIONS}
            \end{raggedright}
                                    
            \begin{enumerate}
            \item Why there are \textbf{5 options} in the OMR Sheet?
            \begin{itemize}
            \item We appreciate your genuineness, if you don't know the answer you can always mark \textbf{Option E}.
            \item Option E, represents you \textbf{will learn that concept later.}
            \item The objective of this LaPIS diagnostic test is to help you identify and bridge the unknown concepts not to evaluate how much you're scoring.
            \end{itemize}
            \item Where can I find the \textbf{Roll Number}?
            \begin{itemize}
            \item Roll number is a \textbf{6 - Digit number} in your \textbf{LaPIS hall ticket}.
            \end{itemize}
            \item How to fill the \textbf{question paper code}?
            \begin{itemize}
            \item Question paper code is a \textbf{3 - Digit number} in the \textbf{top of this page}.
            \end{itemize}
            \item General Instructions
            \begin{itemize}
            \item The question paper consists of \textbf{'''+str(self.total_marks)+r''' Questions}.
            \item Each question carries \textbf{1 mark}.
            \item Total duration of the diagnostic test is \textbf{1 hour 30 min}.
            \item All questions in the booklet are \textbf{objective type}.
            \item For each question, five options are provided whereas \textbf{option E,} indicates Yet to learn / I will learn later.
            \end{itemize}
            \end{enumerate}
            \rule{\textwidth}{1pt}
            '''))

            # \begin{raggedright}
            # \vspace{3mm}
            # \textbf{\large INSTRUCTIONS}
            # \vspace{5mm}
            # \end{raggedright}}
        else:
            self.doc.append(NoEscape(r'\rule{\textwidth}{1pt}'))
        # self.doc.append(NoEscape(r'\newpage'))

    def add_question(self,is_option_shuffled,each_question_tag,index):
        """_summary_

        Args:
            each_question_tag (_type_): _description_
            index (_type_): To pass the index as question number
            conn (_type_): _description_
            cursor (_type_): _description_
        """        """"""
        
        #self.doc.append(NoEscape(r'\ifnum\value{page}<8'))   # Insert Latex page limit check

        print(each_question_tag)
        df = self.question_df.loc[self.question_df['lapis_question_tag'] == each_question_tag.replace("–","-")]
  
        # question_data = df['raw_latex_data'].values[0]
        # correct_answer = df['correct_answer'].values[0]
        
        
        question_data_from_df = df['question_data'].values[0]
        question_data = question_data_from_df['raw_latex_data']
        correct_answer = question_data_from_df['correct_answer']

        self.original_options = ['A', 'B', 'C', 'D']
        
        if is_option_shuffled:  #not implemented
          print(is_option_shuffled)


          # Prepare the shuffled options list        
          self.shuffled_options = self.original_options.copy()
    
          if self.code == 'B':
            # Shift a option 1 time (A->B B->A)
            question_data = question_data.replace('optionA','temp')
            question_data = question_data.replace('optionB','optionA')
            question_data = question_data.replace('temp','optionB')
            print(f'{self.qp_code}-{self.code}')
            self.shuffled_options = ['B', 'A', 'C', 'D']

          elif self.code == 'C':
              # Shift a option 2 time  (A->C, B->D, C->A, D->B)
              question_data = question_data.replace('optionA', 'TEMP_A')
              question_data = question_data.replace('optionB', 'TEMP_B')
              question_data = question_data.replace('optionC', 'optionA')
              question_data = question_data.replace('optionD', 'optionB')
              question_data = question_data.replace('TEMP_A', 'optionC')
              question_data = question_data.replace('TEMP_B', 'optionD')
              print(f'{self.qp_code}-{self.code}')
              self.shuffled_options = ['C', 'D', 'A', 'B']

          elif self.code == 'D':
              # Shift a option 3 time (A->D, B->C, C->B, D->A)
              question_data = question_data.replace("optionD", "TEMP_A") 
              question_data = question_data.replace("optionC", "TEMP_B")  
              question_data = question_data.replace("optionB", "optionC") 
              question_data = question_data.replace("optionA", "optionD") 
              question_data = question_data.replace("TEMP_B", "optionB")  
              question_data = question_data.replace("TEMP_A", "optionA")
              print(f'{self.qp_code}-{self.code}')
              self.shuffled_options = ['D', 'C', 'B', 'A']

        
        question_number_pattern =r"questionnumber(\s)*=(\s)*\{(.+?)\n?\}"
        question_data = re.sub(question_number_pattern, "questionnumber={" + str(index+1) + "}", question_data)
        #print(question_data)
        
        
        if question_data[-1] == ',':
            question_data = question_data[:-1]

        self.doc.append(NoEscape(r'''
                                    \begin{minipage}{\textwidth}
                                    '''))
        self.doc.append(NoEscape(question_data))
        self.doc.append(NoEscape(r'''
                                    \end{minipage}
                                    '''))

        #self.doc.append(NoEscape(r'\fi'))  # End the Latex page limit condition

        self.output_df.loc[len(self.output_df)] = [self.qp_code,index+1,df['lapis_question_tag'].values[0]]

        

    def conn_to_lb_db(self):
       try:
          conn = py.connect(
              dbname=self.LB_DB_DATABASE_NAME,
              user=self.LB_DB_USER_NAME,
              password=self.LB_DB_PASSWORD,
              host=self.LB_DB_HOST_NAME_OR_IP,
              port=self.LB_DB_PORT
          )
          cur = conn.cursor()
       except Exception as e:
          print('conn error in lb db',e)

       return conn,cur
       

    def insert_the_pdf_data_to_DB(self):  
      try:
          

          #self.lapis_roll_number = None
          self.cur = self.conn.cursor()
          # the conflict column name is temp after updating the roll number we change to roll number
          query="""INSERT INTO exam.exam_document_track(document_type_id,
                                              exam_document_level,
                                              roll_number,
                                              section_id,
                                              class,
                                              school_id,
                                              status,
                                              document_name,
                                              extra_data,
                                              is_additional_document,
                                              is_additional_document_used,
                                              created_by)
          values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
          ON CONFLICT (exam_document_track_id) DO UPDATE SET
           document_type_id = EXCLUDED.document_type_id,
           exam_document_level = EXCLUDED.exam_document_level,
           roll_number = EXCLUDED.roll_number,   
           section_id = EXCLUDED.section_id,
           class = EXCLUDED.class,
           school_id = EXCLUDED.school_id,
           status = EXCLUDED.status,
           document_name = EXCLUDED.document_name,
           extra_data = EXCLUDED.extra_data,
           is_additional_document = EXCLUDED.is_additional_document,
           is_additional_document_used = EXCLUDED.is_additional_document_used,
           created_by = EXCLUDED.created_by
          RETURNING exam_document_track_id"""
          
          extra_data = { "question_paper_variant": self.code}

          extra_data_json = json.dumps(extra_data)
          
          # for roll number  = self.lapis_roll_number
          # for section_id  = self.section_id
          if self.section_id:

            track_id = db.execute_query_and_return_result(query=query,vars=(1,'Student', self.lapis_roll_number,self.section_id,self.std,self.school_id,'Generated',f'{self.lapis_roll_number}-{self.qp_code}-{self.code}',extra_data_json,'False','False',47))

            #self.cur.execute(query,(1,'Student', self.lapis_roll_number,self.section_id,self.std,self.school_id,'Generated',f'{self.lapis_roll_number}-{self.qp_code}-{self.code}',extra_data_json,'False','False',47))
          else:
             
             track_id = db.execute_query_and_return_result(query=query,vars=(1,'Student',None,None,self.std,self.school_id,'Generated',f'{self.qp_code}-{self.code}',extra_data_json,'True','False',47))

             #self.cur.execute(query,(1,'Student',None,None,self.std,self.school_id,'Generated',f'{self.qp_code}-{self.code}',extra_data_json,'True','False',47))
          
          #self.conn.commit()

          self.exam_doc_track_id = track_id[0][0]
          print('sucessfully inserted the pdf Data to DB')

      except Exception as e:
          print('Error Inserting the pdf data',e)
          self.conn.rollback()


    def generate_barcode(self):
        """_summary_

        Args:
            data (_type_): data for the Barcode
            roll_number (int): For create a Barcode uniquely

        Returns:
            _type_: _description_
        """
        
        self.barcode_path = f"lapis_uploader/{self.folder_name}"
        
        if self.section_id:
          self.barcode_filename = os.path.join(self.barcode_path, f'barcode{self.lapis_roll_number}-{self.exam_doc_track_id}')
        else:
          self.barcode_filename = os.path.join(self.barcode_path, f'barcode-{self.exam_doc_track_id}')

        code = barcode.get('code128',str(self.exam_doc_track_id), writer=ImageWriter())
        
        code.save(self.barcode_filename,options={"write_text": False})
       
        #return f'{self.barcode_filename}.png'  # Assuming barcode saves as PNG


    def update_the_column_in_db(self,link_data):
       """_summary_

      Args:
          link_data (str): pdf minio url

      Raises:
          Exception: _description_
       """       
       
       try:
         
          query = """UPDATE exam.exam_document_track 
                  SET document_url = %s 
                  where exam_document_track_id = %s"""
          
          db.execute_query(query=query,vars=(link_data,self.exam_doc_track_id))
          #self.cur.execute(query,(link_data,self.exam_doc_track_id))
          
          #self.conn.commit()
          print('Updated the pdf url to DB')

       except Exception as e:
          
          print('Error Updating the pdf url',e)
          
          self.conn.rollback()
          raise Exception


    
    def insert_the_options_to_db(self,original_options, shuffled_options):
        """_summary_

        Args:
            original_options (str): OG Options
            shuffled_options (str): Shuffled options

        Returns:
            _type_: _description_
        """        
        try:
          self.cur = self.conn.cursor()
        
              
          for index, (orig_opt, shuffled_opt) in enumerate(zip(original_options, shuffled_options)):
              option_insert_query =""" INSERT INTO exam.shuffle_option_detail
              (shuffle_question_id, base_option, shuffled_option, created_by)
              values(%s,%s,%s,%s)
              ON CONFLICT (shuffle_question_id,base_option ) DO UPDATE SET 
                  shuffled_option = EXCLUDED.shuffled_option,
                  created_by = EXCLUDED.created_by """
                                
              db.execute_query(option_insert_query,(self.shuffle_question_id,orig_opt,shuffled_opt,47))   
              #self.cur.execute(option_insert_query,(self.shuffle_question_id,orig_opt,shuffled_opt,47))      
              print(self.shuffle_question_id) 
          
          #self.conn.commit()

        except Exception as e:
          print(f"Error connecting to the database: {e}")
          self.conn.rollback()
         

        
    def insert_the_questions_to_db(self, original_question_num, shuffled_question_num):
        """_summary_

        Args:
            original_question_num (int): OG Question number
            shuffled_question_num (int): Shuffled Question number

        Returns:
            _type_: _description_
        """        """"""
        try:
          self.cur = self.conn.cursor()
          question_insert_query = """ INSERT INTO exam.shuffle_question_detail 
            (shuffle_question_paper_code, base_question_paper_question_id, shuffle_question_number, created_by)
            values (%s,%s,%s,%s)
            ON CONFLICT (shuffle_question_paper_code, base_question_paper_question_id) DO UPDATE SET   
                      shuffle_question_number = EXCLUDED.shuffle_question_number, 
                      created_by = EXCLUDED.created_by  
            RETURNING shuffle_question_id     
            """   

          shuffle_question_id = db.execute_query_and_return_result(question_insert_query,(f'{self.qp_code}-{self.code}',original_question_num,shuffled_question_num,47))       
          #self.cur.execute(question_insert_query,(f'{self.qp_code}-{self.code}',original_question_num,shuffled_question_num,47))       
          self.shuffle_question_id = shuffle_question_id[0][0]
          print(f'shuffle_question_id--{self.shuffle_question_id}')      
          
          #self.conn.commit()
        
        except Exception as e:
          print(f"Error connecting to the database: {e}")
          self.conn.rollback()


    def add_data(self,is_question_shuffled,is_option_shuffled):
        """_summary_

      Args:
          is_question_shuffled (bool): true or false
          is_option_shuffled (bool):  true or false
      """        
        
        #conn,cursor = create_connection()
        
        self.output_df = pd.DataFrame(columns=['qp_code','question_number','question_tag'])

        basic_questions = self.question_df.loc[self.question_df['lapis_question_tag'].str.contains('b', case=False)]

        normal_question = self.question_df.loc[self.question_df['lapis_question_tag'].str.contains('dt', case=False)]

        critical_question = self.question_df.loc[self.question_df['lapis_question_tag'].str.contains('ct', case=False)]
        
        print(len(basic_questions))
        print(len(normal_question))
        print(len(critical_question))

        
        if is_question_shuffled:  
          print(is_question_shuffled)

          
          self.shuffled_questions = {
            'A': {'basic': pd.DataFrame(), 'normal': pd.DataFrame(), 'critical': pd.DataFrame()},
            'B': {'basic': pd.DataFrame(), 'normal': pd.DataFrame(), 'critical': pd.DataFrame()},
            'C': {'basic': pd.DataFrame(), 'normal': pd.DataFrame(), 'critical': pd.DataFrame()},
            'D': {'basic': pd.DataFrame(), 'normal': pd.DataFrame(), 'critical': pd.DataFrame()}
        }
          
          if not self.section_id:

            self.shuffled_questions[self.code]['basic'] = basic_questions.sample(frac=1).reset_index(drop=True)
            self.shuffled_questions[self.code]['normal'] = normal_question.sample(frac=1).reset_index(drop=True)
            self.shuffled_questions[self.code]['critical'] = critical_question.sample(frac=1).reset_index(drop=True)

            
            self.shuffled_questions[self.code]['basic'].to_csv(f'shuffled_basic_questions_{self.code}.csv', index=False)
            self.shuffled_questions[self.code]['normal'].to_csv(f'shuffled_normal_questions_{self.code}.csv', index=False)
            self.shuffled_questions[self.code]['critical'].to_csv(f'shuffled_critical_questions_{self.code}.csv', index=False)
            print(f"Shuffled questions saved to CSV files.")

            # Call the csv upload function
            self.upload_csv_to_minio()

          # Load the shuffled questions from the CSV files
          self.shuffled_questions[self.code]['basic'] = pd.read_csv(f'shuffled_basic_questions_{self.code}.csv')
          self.shuffled_questions[self.code]['normal'] = pd.read_csv(f'shuffled_normal_questions_{self.code}.csv')
          self.shuffled_questions[self.code]['critical'] = pd.read_csv(f'shuffled_critical_questions_{self.code}.csv')
          print(f"Shuffled questions loaded from CSV files.")

          # Select the shuffled questions based on the code
          self.basic_questions_shuffled = self.shuffled_questions[self.code]['basic']
          self.normal_question_shuffled = self.shuffled_questions[self.code]['normal']
          self.critical_question_shuffled = self.shuffled_questions[self.code]['critical']

          #conn.commit()

          for  (og_index,n_row),(index,row) in zip(basic_questions.iterrows(),self.basic_questions_shuffled.iterrows()):
              self.add_question(is_option_shuffled,each_question_tag=row['lapis_question_tag'],index=og_index)

          for (og_index,n_row),(index,row) in zip(normal_question.iterrows(),self.normal_question_shuffled.iterrows()):
              self.add_question(is_option_shuffled,each_question_tag=row['lapis_question_tag'], index = og_index)
    
        
          if len(self.critical_question_shuffled ) > 0:
              self.doc.append(NoEscape(r'\vspace{0.5cm}'))
              self.doc.append(NoEscape(r'''
                                      \begin{minipage}{\textwidth}
                                      '''))
              self.doc.append(NoEscape(r' \Centering \Large \textbf{Critical Thinking Questions}'))
              self.doc.append(NoEscape(r'''
                                      \end{minipage}
                                      '''))
              for (og_index,n_row),(index,row) in zip(critical_question.iterrows(),self.critical_question_shuffled.iterrows()):
                  self.add_question(is_option_shuffled,each_question_tag=row['lapis_question_tag'],index= og_index)

          # self.doc.append(NoEscape(r'\newgeometry{top=1cm,left=1cm,right=1cm,bottom=1.5cm}'))
       
        else:
           
          for index ,row in basic_questions.iterrows():
              self.add_question(is_option_shuffled,each_question_tag=row['lapis_question_tag'],index=index)

          for index ,row in normal_question.iterrows():
              self.add_question(is_option_shuffled,each_question_tag=row['lapis_question_tag'], index = index)
    
        
          if len(critical_question) > 0:
              self.doc.append(NoEscape(r'\vspace{0.5cm}'))
              self.doc.append(NoEscape(r'''
                                      \begin{minipage}{\textwidth}
                                      '''))
              self.doc.append(NoEscape(r' \Centering \Large \textbf{Critical Thinking Questions}'))
              self.doc.append(NoEscape(r'''
                                      \end{minipage}
                                      '''))
              for index ,row in critical_question.iterrows():
                  self.add_question(is_option_shuffled,each_question_tag=row['lapis_question_tag'],index= index)

          # self.doc.append(NoEscape(r'\newgeometry{top=1cm,left=1cm,right=1cm,bottom=1.5cm}'))
           
        #conn.commit()
        self.output_df.to_csv(f"{self.qp_code}_{self.code}_class_{self.std}_{self.subject}_answerkey.csv",index=False)
        # self.doc.change_document_style("bodypage")
        

        # Call the main queston and option insert to db function
        if not self.section_id:
           
          #self.insert_the_questions_and_options_to_DB(basic_questions,normal_question,critical_question,is_question_shuffled,is_option_shuffled)
          print('Section ID not Given')    
        else:
           print('Section ID Gave')

    def insert_the_questions_and_options_to_DB(self,basic_questions,normal_question,critical_question,is_question_shuffled,is_option_shuffled):
          """_summary_

      Args:
          basic_questions (_type_): Iterate over the basic questions dataframe to fetch the index
          normal_question (_type_): Iterate over the normal questions dataframe to fetch the index
          critical_question (_type_): Iterate over the critical questions dataframe to fetch the index
          is_question_shuffled (bool): _description_
          is_option_shuffled (bool): _description_
      """
          # For the basic questions 
          print('Start inserting the Shuffled QuestionNumber to the DB')
          if is_question_shuffled: 
             
            basic_questions_shuffl  = self.basic_questions_shuffled
            for (orig_row, shuffled_row),(basic,b_shuffled) in zip(basic_questions.iterrows(), basic_questions_shuffl.iterrows()):
              original_question_num = orig_row+1
              shuffled_question_num = b_shuffled['base_question_number'] # After shuffle

              # Call the question insert function
              print(f'original_question_num-{original_question_num}')
              print(f'shuffled_question_num-{shuffled_question_num}')
              self.insert_the_questions_to_db(original_question_num, shuffled_question_num)
              
              
              #call the option insert function
              if is_option_shuffled:
                print('Start Inserting the shuffled options to the DB')
                self.insert_the_options_to_db(self.original_options, self.shuffled_options)
                print("Shuffled options:", self.shuffled_options)
              else:
                self.insert_the_options_to_db(self.original_options, self.original_options)


            #For the normal questions
            normal_question_shuffl  = self.normal_question_shuffled
            for (orig_row, shuffled_row),(normal,n_shuffled) in zip(normal_question.iterrows(), normal_question_shuffl.iterrows()):
              original_question_num = orig_row+1
              shuffled_question_num = n_shuffled['base_question_number'] # After shuffle
              

              # Call the question insert function
              print(f'original_question_num-{original_question_num}')
              print(f'shuffled_question_num-{shuffled_question_num}')
              self.insert_the_questions_to_db(original_question_num, shuffled_question_num)
             
              
              #call the option insert function
              if is_option_shuffled:
                print('Start Inserting the shuffled options to the DB')
                self.insert_the_options_to_db(self.original_options, self.shuffled_options)
                print("Shuffled options:", self.shuffled_options)
              else:
                self.insert_the_options_to_db(self.original_options, self.original_options)

            #For the critical questions
            critical_question_shuffl  = self.critical_question_shuffled
            for (orig_row, shuffled_row),(critical,c_shuffled) in zip(critical_question.iterrows(), critical_question_shuffl.iterrows()):
              original_question_num = orig_row+1
              shuffled_question_num = c_shuffled['base_question_number'] # After shuffle

              # Call the question insert function
              print(f'original_question_num-{original_question_num}')
              print(f'shuffled_question_num-{shuffled_question_num}')
              self.insert_the_questions_to_db(original_question_num, shuffled_question_num)
 
              #call the option insert function
              if is_option_shuffled:
                print('Start Inserting the shuffled options to the DB')
                self.insert_the_options_to_db(self.original_options, self.shuffled_options)
                print("Shuffled options:", self.shuffled_options)
              else:
                self.insert_the_options_to_db(self.original_options, self.original_options)

          else:
             
             #for basic questions
             for orig_row, normal_row in basic_questions.iterrows():
              original_question_num = orig_row+1

              # Call the question insert function
              self.insert_the_questions_to_db(original_question_num, original_question_num)
              
              #call the option insert function
              if is_option_shuffled:
                print('Start Inserting the shuffled options to the DB')
                self.insert_the_options_to_db(self.original_options, self.shuffled_options)
                print("Shuffled options:", self.shuffled_options)
              else:
                self.insert_the_options_to_db(self.original_options, self.original_options)


             #For the normal questions
             for orig_row, normal_row in normal_question.iterrows():
              original_question_num = orig_row+1

              # Call the question insert function
              self.insert_the_questions_to_db(original_question_num, original_question_num)
              
              #call the option insert function
              if is_option_shuffled:
                print('Start Inserting the shuffled options to the DB')
                self.insert_the_options_to_db(self.original_options, self.shuffled_options)
                print("Shuffled options:", self.shuffled_options)
              else:
                self.insert_the_options_to_db(self.original_options, self.original_options)


             #For the critical questions
             for orig_row, normal_row in critical_question.iterrows():
              original_question_num = orig_row+1

              # Call the question insert function
              self.insert_the_questions_to_db(original_question_num, original_question_num)
              
              #call the option insert function
              if is_option_shuffled:
                print('Start Inserting the shuffled options to the DB')
                self.insert_the_options_to_db(self.original_options, self.shuffled_options)
                print("Shuffled options:", self.shuffled_options)
              else:
                self.insert_the_options_to_db(self.original_options, self.original_options)
             
    def add_watermark(self):
      
        new_comm = UnsafeCommand('newcommand', '\hdrb', options=2,extra_arguments=r'''
            {\Large \centering \textbf{#1}} \\ 
            
                ''')
        
        # new_comm = UnsafeCommand('newcommand', '\hdrb', options=2,extra_arguments=r'''
        #     #1 \\
        #     #2
            
        #         ''')
        self.doc.preamble.append(new_comm)
      
        bodypage = PageStyle("bodypage")
        emptypage = PageStyle("emptypage")

        with emptypage.create(Foot("R")):
            emptypage.append(NoEscape(r"""\vspace{0cm}"""))
            emptypage.append(NoEscape(r"Page \thepage"))
            # bodypage.append(NoEscape(r"""\hspace{3cm}"""))

        with emptypage.create(Foot("C")):
            emptypage.append(NoEscape(r"""\vspace{0cm}"""))
            emptypage.append(NoEscape(f"www.learnbasics.fun"))

        with emptypage.create(Head("C")):
            emptypage.append(NoEscape(r"""\vspace{0cm}"""))
            if self.is_interest_groups:
                emptypage.append(NoEscape(f"Bridge Course Completion Test"))
            
        if not self.is_final:
          with emptypage.create(Head("R")):
              # emptypage.append(NoEscape(r"""Right Header"""))
              emptypage.append(NoEscape(datetime.now().strftime("%d-%m-%Y %H:%M:%S")))

        with emptypage.create(Foot("L")):
            if self.is_interest_groups:
                emptypage.append(NoEscape(f"Question Paper Code - {self.qp_code} - {self.code}"))
            else:
                emptypage.append(NoEscape(f"LaPIS - {self.qp_code} - {self.code}"))

        #with bodypage.create(Foot("R")):
           # bodypage.append(NoEscape(r"""\vspace{0cm}"""))
            #bodypage.append(NoEscape(r"Page \thepage"))
            # bodypage.append(NoEscape(r"""\hspace{3cm}"""))

            # Conditional for placing the barcode only on the first page
        if self.section_id:
              bodypage.append(NoEscape(r'''
    \ifnum\value{page}=1
        \fancyfoot[R]{%
            \vspace{0cm}%
            \includegraphics[width=4cm,height=0.5cm]{barcode''' + f"{self.lapis_roll_number}-{self.exam_doc_track_id}.png" + r'''}
        }
    \else
        \fancyfoot[R]{%
            \vspace{0cm}%
            Page No - \thepage
        }
    \fi
'''))

        else:
               bodypage.append(NoEscape(r'''
    \ifnum\value{page}=1
        \fancyfoot[R]{%
            \vspace{0cm}%
            \includegraphics[width=4cm,height=0.5cm]{barcode''' + f"-{self.exam_doc_track_id}.png" + r'''}
        }
    \else
        \fancyfoot[R]{%
            \vspace{0cm}%
            Page No - \thepage
        }
    \fi
'''))
               


        emptypage.append(pl.Command('renewcommand', arguments=[pl.NoEscape(r'\footrulewidth'),'1pt']))
        
        
        if self.is_interest_groups:
          with bodypage.create(Head("L")):
              bodypage.append(NoEscape(r"""\includegraphics[width = 3.0cm, height=1.125cm]{logo.png}"""))

          with bodypage.create(Head("C")):
              bodypage.append(NoEscape(r"\hdrb{"+self.school_name+r"}{Enhanced by Learn Basics}"))
        
        
        
        with bodypage.create(Foot("C")):
            bodypage.append(NoEscape(r"""\vspace{0cm}"""))
            bodypage.append(NoEscape(f"www.learnbasics.fun"))
      
        with bodypage.create(Foot("L")):
            if self.is_interest_groups:
                bodypage.append(NoEscape(f"Bridge Course Completion Test - {self.qp_code} - {self.code}"))
            else:
                bodypage.append(NoEscape(f"LaPIS - {self.qp_code} - {self.code}"))

        # with bodypage.create(Head("C")):
        #     bodypage.append(NoEscape(r"Enhanced by Learn Basics"))
                    
        bodypage.append(pl.Command('renewcommand', arguments=[pl.NoEscape(r'\footrulewidth'),'1pt']))
        bodypage.append(pl.Command('renewcommand', arguments=[pl.NoEscape(r'\headrulewidth'),'0pt']))
        
        emptypage.append(pl.Command('renewcommand', arguments=[pl.NoEscape(r'\footrulewidth'),'1pt']))
        emptypage.append(pl.Command('renewcommand', arguments=[pl.NoEscape(r'\headrulewidth'),'1pt']))

        emptypage.append(NoEscape(r'\setlength{\headsep}{0.5cm}'))
        self.doc.preamble.append(bodypage)
        
        self.doc.preamble.append(emptypage)

        self.doc.change_page_style("bodypage")

        if self.is_final:
          self.doc.append(NoEscape(r'''\SetWatermarkLightness{ 0.95 }
          \SetWatermarkText{Learn Basics}
          \SetWatermarkScale{ 0.7 }'''))
        else:
           self.doc.append(NoEscape(r'''\SetWatermarkLightness{ 0.95 }
          \SetWatermarkText{Draft Question Paper}
          \SetWatermarkScale{ 0.5 }'''))
        
        if self.is_interest_groups:
          self.doc.append(NoEscape(r'''\setlength{\headsep}{0.5cm}'''))
        else:
          self.doc.append(NoEscape(r'''\setlength{\headsep}{0cm}'''))

    def download_images(self):
        client = create_client()                              

        # list_of_images_to_download = client.list_objects(bucket_name='lapis-question-paper-images',prefix=self.folder_name)
        list_of_images_to_download = client.list_objects(bucket_name='lapis-question-paper-images',recursive=True,prefix=f"C{self.std}{self.subject[0].upper()}")


        os.makedirs(name=f'lapis_uploader/{self.folder_name}',exist_ok=True)
        
        for each_file in list_of_images_to_download:
          pre_url = client.get_presigned_url(bucket_name='lapis-question-paper-images',object_name=f"{each_file.object_name}",method='GET')
          response = requests.get(url=pre_url)
          
          image_name = each_file.object_name.split("/")[-1]

          with open(f"lapis_uploader/overleaf_files/question_papers/{image_name}",'wb') as file:

            file.write(response.content)
        
        list_of_images_to_download = client.list_objects(bucket_name='lapis-question-paper-images',recursive=True,prefix=f"C6M")
        
        for each_file in list_of_images_to_download:
          pre_url = client.get_presigned_url(bucket_name='lapis-question-paper-images',object_name=f"{each_file.object_name}",method='GET')
          response = requests.get(url=pre_url)
          
          image_name = each_file.object_name.split("/")[-1]
 
          with open(f"lapis_uploader/overleaf_files/question_papers/{image_name}",'wb') as file:
            
            file.write(response.content)
          # client.fget_object(file_path=f"lapis_uploader/overleaf_files/{each_file.object_name}")
            
    def generate(self,row,code,is_question_shuffled,is_option_shuffled, download_images=True, upload_required=True):
      self.upload_required = upload_required
      
      if download_images:
          self.download_images()
      data = self.generate_question_paper(row,code,is_question_shuffled,is_option_shuffled)
      return data
        
    def generate_question_paper(self,row,code,is_question_shuffled,is_option_shuffled):
        """_summary_

        Args:
            row (df): iterrate from the dataframe
            code (str): to pass the sub code
            is_question_shuffled (bool): to shuffle the questions
            is_option_shuffled (bool): to shuffle the options
 
        Returns:
            _type_: _description_
        """
        results = []  # To store the results (success or error) for each qp_code
     
        link_data = None #Incase if the upload or generating  is error 

        self.is_question_shuffled = is_question_shuffled

        self.is_option_shuffled = is_option_shuffled

        self.code = code  # Alternating between A, B, C, D
        

        if self.section_id:  
          
              
              self.lapis_roll_number = row['lapis_roll_number']
              #self.is_interest_group = row['is_interest_group'] 
            

              # Insert the data to the db to get the exam document track id
              self.insert_the_pdf_data_to_DB()

              # after insert the data call the barcode generation function
              self.generate_barcode()

              

              geometry_options = {"top": "1.5cm", "bottom": "1cm", "left": "1cm", "right": "1cm"}
              self.doc = Document(geometry_options=geometry_options)
              self.doc.documentclass = Command('documentclass', options=['11pt', 'a4paper'], arguments=['article'])

              self.get_lib()
              self.add_custom_commands()
              self.get_questions()
              self.add_watermark()

              if self.is_interest_groups:
                  self.add_header_bridge_course()
              else:
                  self.add_header_regular()

              self.add_instuctions()
              self.add_data(is_question_shuffled ,is_option_shuffled)

              try:
                  # Generate the PDF for the current qp_code
                  file_path = f"lapis_uploader/{self.folder_name}/{self.lapis_roll_number}-{self.qp_code}-{self.code}"
                  self.doc.generate_pdf(filepath=file_path, compiler='pdflatex', clean_tex=False)

                  if self.upload_required:
                      link_data = self.upload_to_minio()
                      results.append({
                          "message": f"Generated {self.lapis_roll_number}-{self.qp_code}-{self.code}",
                          "link": link_data
                      })

                      link_data = self.pdf_link

                  else:
                      results.append({
                          "message": f"Generated {self.lapis_roll_number}-{self.qp_code}-{self.code}, Upload to Minio not requested"
                      })

              except Exception as e:
                  # Handle LaTeX generation errors
                  client = create_client()
                  tex_file_path = f"lapis_uploader/{self.folder_name}/{self.lapis_roll_number}-{self.qp_code}-{self.code}.tex"
                  client.fput_object(bucket_name='lapis', object_name=f"question_papers/{self.lapis_roll_number}-{self.qp_code}-{self.code}.tex", file_path=tex_file_path)

                  tex_link = client.get_presigned_url(bucket_name='lapis', method='GET', object_name=f"question_papers/{self.lapis_roll_number}-{self.qp_code}-{self.code}.tex")

                  results.append({
                      "message": f"Question Paper {self.lapis_roll_number}-{self.qp_code}-{self.code} Failed due to LaTeX Error",
                      "tex_link": tex_link
                  })

              #self.insert_the_pdf_data_to_DB(link_data)  #For insert the pdf data to the DB

        else:
              

              # Insert the data to the db to get the exam document track id
              self.insert_the_pdf_data_to_DB()

              # after insert the data call the barcode generation function
              self.generate_barcode()

         
              geometry_options = {"top": "1.5cm", "bottom": "1cm", "left": "1cm", "right": "1cm"}
              self.doc = Document(geometry_options=geometry_options)
              self.doc.documentclass = Command('documentclass', options=['11pt', 'a4paper'], arguments=['article'])
              
              self.get_lib()
              self.add_custom_commands()
              self.get_questions()
              self.add_watermark()
              
              if self.is_interest_groups:
                  self.add_header_bridge_course()
              else:
                  self.add_header_regular()
              
              self.add_instuctions()

              self.add_data(is_question_shuffled ,is_option_shuffled)

              try:
                  # Generate the PDF for the current qp_code   
                 

                  file_path = f"lapis_uploader/{self.folder_name}/{self.qp_code}-{self.code}"
                                      
                  self.doc.generate_pdf(filepath=file_path, compiler='pdflatex', clean_tex=False)
                  
                  if self.upload_required:
                      link_data = self.upload_to_minio()
                      results.append({
                          "message": f"Generated {self.qp_code}-{self.code}",
                          "link": link_data
                      })

                      link_data = self.pdf_link

                  else:
                      results.append({
                          "message": f"Generated {self.qp_code}-{self.code}, Upload to Minio not requested"
                      })

              except Exception as e:
                  # Handle any LaTeX errors, upload the .tex file instead
                  client = create_client()
                  tex_file_path = f"lapis_uploader/{self.folder_name}/{self.qp_code}-{self.code}.tex"
                  client.fput_object(bucket_name='lapis', object_name=f"question_papers/{self.qp_code}-{self.code}.tex", file_path=tex_file_path)
                  
                  tex_link = client.get_presigned_url(bucket_name='lapis', method='GET', object_name=f"question_papers/{self.qp_code}-{self.code}.tex")
                  
                  results.append({
                      "message": f"Question Paper {self.qp_code}-{self.code} Failed due to LaTeX Error",
                      "tex_link": tex_link
                  })

              #self.insert_the_pdf_data_to_DB(link_data)  #For insert the pdf data to the DB  
        
        self.update_the_column_in_db(link_data)  #For update the pdf url to the DB
        
        return results  

    def upload_csv_to_minio(self):
      # Upload the CSV file to Minio

      client = create_client()
      try:
          
          #Storing the csv in the minio for the future references and creating for single student and for creating with roll number students
          client.fput_object(bucket_name='lapis',object_name = f"question_papers/shuffled_basic_questions_{self.code}.csv",file_path=f"shuffled_basic_questions_{self.code}.csv")
          client.fput_object(bucket_name='lapis',object_name = f"question_papers/shuffled_normal_questions_{self.code}.csv",file_path=f"shuffled_normal_questions_{self.code}.csv")
          client.fput_object(bucket_name='lapis',object_name = f"question_papers/shuffled_critical_questions_{self.code}.csv",file_path=f"shuffled_critical_questions_{self.code}.csv")

          print("CSV file moved to Minio")

      except Exception as e:
         print(e)
         raise Exception ("Failed to upload CSV to Minio")  

        
    def upload_to_minio(self):  
      client = create_client()
      try:
          
          if not self.section_id:
            # for upload the without roll number pdf 
            client.fput_object(bucket_name='lapis',object_name = f"question_papers/{self.qp_code}-{self.code}.pdf",file_path=f"lapis_uploader/{self.folder_name}/{self.qp_code}-{self.code}.pdf")
            client.fput_object(bucket_name='lapis',object_name = f"question_papers/{self.qp_code}-{self.code}.tex",file_path=f"lapis_uploader/{self.folder_name}/{self.qp_code}-{self.code}.tex")

            # Get the url of the without roll number PDF
            self.pdf_link = client.get_presigned_url(bucket_name='lapis',method='GET',object_name=f"question_papers/{self.qp_code}-{self.code}.pdf")
            self.tex_link = client.get_presigned_url(bucket_name='lapis',method='GET',object_name=f"question_papers/{self.qp_code}-{self.code}.tex")
            
          else:
            # for upload the PDF with roll number 
            client.fput_object(bucket_name='lapis',object_name = f"question_papers/{self.lapis_roll_number}-{self.qp_code}-{self.code}.pdf",file_path=f"lapis_uploader/{self.folder_name}/{self.lapis_roll_number}-{self.qp_code}-{self.code}.pdf")
            client.fput_object(bucket_name='lapis',object_name = f"question_papers/{self.lapis_roll_number}-{self.qp_code}-{self.code}.tex",file_path=f"lapis_uploader/{self.folder_name}/{self.lapis_roll_number}-{self.qp_code}-{self.code}.tex")
          
            # Get the url of the with roll number PDF
            self.pdf_link = client.get_presigned_url(bucket_name='lapis',method='GET',object_name=f"question_papers/{self.lapis_roll_number}-{self.qp_code}-{self.code}.pdf")
            self.tex_link = client.get_presigned_url(bucket_name='lapis',method='GET',object_name=f"question_papers/{self.lapis_roll_number}-{self.qp_code}-{self.code}.tex")
            
          
          data_to_send =  {
            "message":"Question Paper Generated",
            "pdf_link":self.pdf_link,
            "tex_link":self.tex_link
          }
          
          print(data_to_send)
          
          return data_to_send
          
      except Exception as err:
          print(err)
          raise Exception("failed to move files")

    def get_questions(self): 
       
      self.question_df = processQuery(f"""SELECT
	BASE_QUESTION_PAPER_CODE,
	BASE_QUESTION_NUMBER,
	pool.question_tag as lapis_question_tag,
question_data,
	raw_lATEX_text as raw_lATEX_DATA--,correct_option as correct_answer
FROM exam.base_question_paper_detail lcod 
join content.question_pool pool on pool.question_tag= lcod.question_tag
WHERE BASE_QUESTION_PAPER_CODE = '{self.qp_code}'
order by BASE_QUESTION_NUMBER""")
    
      self.total_marks = self.question_df.shape[0]
      
      if self.question_df. empty:
        raise Exception(f"No Question Selected for Question paper code {self.qp_code}")
      

    def Student_particular(self):
      try:
          
          
          self.cur = self.conn.cursor()

          query = """
          select school_name,roll_number,sd.class,subject,sad.subject_id,sad.is_interest_group

		from exam.exam_detail ed 

        join school_data.school_section_detail sd on sd.section_id = ed.section_id

		join school_data.school_academic_detail sad on sad.section_id = sd.section_id

        join school_data.school_detail ssd on ssd.school_id = sd.school_id

		join school_data.subject_detail sub_d on sub_d.subject_id = sad.subject_id

        join school_data.student_detail sdt on sdt.section_id = ed.section_id

		JOIN exam.exam_qp_code eqc  ON eqc.exam_id = ed.exam_id

		JOIN exam.question_paper_detail qpd ON qpd.question_paper_code = eqc.question_paper_code

        where qpd.question_paper_code  = '%s' and ssd.school_id = %s and sd.section_id =%s and sub_d.subject_id =101
 

                  """          
          rows = db.execute_query_and_return_result(query, (self.qp_code, self.school_id, self.section_id)) 
          #self.cur.execute(query, (self.qp_code, self.school_id, self.section_id))    

          # 905 109 54 - R10 
          # 805 118 115 - R22 
          # 705 118 114 - R29 
               
          #rows = self.cur.fetchall()
          columns = ['school_name', 'lapis_roll_number', 'class', 'subject_name', 'subject_id', 'is_interest_group']
          df = pd.DataFrame(rows,columns=columns)
          return df

      except Exception as e:
          print(f"Error connecting to the database: {e}")
          raise Exception ("Unable to fetch student details")
          

      #finally:
       #   cur.close()
          #conn.close() 

if __name__ == "__main__":
    
    is_question_shuffled = True

    is_option_shuffled = True

    is_final = True

    school_id = 110   

    for qp_code in [200]:
      print("Creating Document without RollNumber")

      qp_codes_sub = ["A", "B", "C", "D"] 

      for code in qp_codes_sub:
       
        row = None 

        section_id = None 
                                
        obj = QuestionPaper(qp_code=qp_code, is_final=is_final, school_id=school_id, section_id=section_id)
        minio_links = obj.generate(download_images=False, upload_required=False,row=row,code=code,is_question_shuffled = is_question_shuffled,is_option_shuffled = is_option_shuffled)
          
    
    for qp_code in [200]:
        
        section_id = 253
                
        obj = QuestionPaper(qp_code=qp_code, is_final=is_final, school_id=school_id, section_id=section_id)
        
        df = obj.Student_particular()
        
        for  i, row in df.iterrows():
                
                code = get_qp_varaint_by_roll_number(row['lapis_roll_number'])
                
                print(f"Processing row {i+1} with roll number: {row['lapis_roll_number']}")
                print(code)
                
                minio_links = obj.generate(row=row,code=code,download_images=False, upload_required=False,is_question_shuffled = is_question_shuffled,is_option_shuffled = is_option_shuffled)
                print(minio_links)
      
          
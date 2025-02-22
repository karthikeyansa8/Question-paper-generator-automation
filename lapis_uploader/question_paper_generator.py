from pylatex import Document,PageStyle, Head, Foot
from pylatex.utils import NoEscape
import pylatex as pl
from pylatex.package import Package
from pylatex import UnsafeCommand,Command,MiniPage
from db_con import processQuery,create_connection,excute_query_without_commit
import os
import re
import pandas as pd
from mino_handler import create_client
import pandas as pd
import random

os.chdir(os.path.dirname(__file__))



class QuestionPaper:
    def __init__(self,qp_code,subject:str,folder_name,std,version,excel_question_df,school_name) -> None:
        self.qp_code = qp_code
        self.subject = subject
        self.version = version
        self.excel_question_df = excel_question_df
        self.school_name = school_name

        self.folder_name = f"overleaf_files/{folder_name}"
        self.std = std


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


    def add_header(self):


        self.doc.preamble.append(NoEscape(r'''%------------------------------------------------------------
        %                    HEADER INFORMATION
        %------------------------------------------------------------
        \newcommand{\hdr}[6]{
        \begin{center}
            {\Large \textbf{#1}}\\\vspace{3mm}
            {\Large #6}\\\vspace{3mm}
            {\Large \textbf{#2\vspace{3mm}\\#3\rule{120pt}{0.5pt}}}\\
        \end{center}
        \begin{raggedleft}
        {#4 \hfill #5 \\}
        \end{raggedleft}                                                          

        }
        '''))

        # self.doc.append(NoEscape(r'\hdr{LaPIS Diagnostic Test - Class '+str(self.std)+r'}{From Learn Basics - www.learnbasics.fun}{'+self.subject.capitalize()+r' - Question Paper Code - '+str(self.qp_code)+r'}{Roll Number }{1 hr 30 min}{Total Questions - '+str(self.total_marks)+r' ,Total Marks - '+str(self.total_marks)+r'}{Swami School - Enhanced by Learn Basics}'))
        self.doc.append(NoEscape(r'\hdr{LaPIS Diagnostic Test - '+self.school_name+r'}{Question Paper Code - '+str(self.qp_code)+r'}{Roll Number }{1 hr 30 min}{Total Questions - '+str(self.total_marks)+r' ,Total Marks - '+str(self.total_marks)+r'}{Class - '+str(self.std)+" - "+self.subject.capitalize()+r'}'))
    
    def add_instuctions(self):

        self.doc.change_page_style("emptypage")

        if not self.subject == 'Science':
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

    def add_question(self,each_question_tag,index,conn,cursor):
        print(each_question_tag)
        df = self.question_df.loc[self.question_df['lapis_question_tag'] == each_question_tag.replace("–","-")]
        
        # print(df)
        question_data = df['raw_latex_data'].values[0]

        question_number = df['question_number'].values[0]

        question_data = question_data.replace("questionnumber={"+str(question_number)+"}","questionnumber={"+str(index+1)+"}")
        question_data = question_data.replace("questionnumber={"+str(question_number)+"}","questionnumber={"+str(index+1)+"}")
        
        question_data = question_data.replace("questionnumber={ "+str(question_number)+"}","questionnumber={"+str(index+1)+"}")
        question_data = question_data.replace("questionnumber={ "+str(question_number)+"}","questionnumber={"+str(index+1)+"}")
        
        question_data = question_data.replace("questionnumber = {"+str(question_number)+" }","questionnumber = {"+str(index+1)+"}")
        question_data = question_data.replace("questionnumber = {"+str(question_number)+" }","questionnumber = {"+str(index+1)+"}")

        question_data = question_data.replace("questionnumber = {"+str(question_number)+"}","questionnumber = {"+str(index+1)+"}")
        question_data = question_data.replace("questionnumber = {"+str(question_number)+"}","questionnumber = {"+str(index+1)+"}")

        if question_data[-1] == ',':
            question_data = question_data[:-1]
            
        self.doc.append(NoEscape(r'''
                                    \begin{minipage}{\textwidth}
                                    '''))
        self.doc.append(NoEscape(question_data))
        self.doc.append(NoEscape(r'''
                                    \end{minipage}
                                    '''))


        self.output_df.loc[len(self.output_df)] = [self.qp_code,index+1,df['lapis_question_tag'].values[0],df['correct_answer'].values[0]]

        query = f"""insert into lapis.lapis_correct_options_detail (base_question_paper_code, question_id,base_question_number,correct_option,subject)
        values (%(base_question_paper_code)s, %(question_id)s,%(base_question_number)s,%(correct_option)s,%(subject)s)
        on conflict (base_question_paper_code, question_id) do update 
        set base_question_number = EXCLUDED.base_question_number,
        subject = EXCLUDED.subject,
        base_question_paper_code = EXCLUDED.base_question_paper_code,
        correct_option = EXCLUDED.correct_option"""
        
        args = {
            "base_question_paper_code":self.qp_code,
            "question_id":each_question_tag.replace("–","-"),
            "base_question_number":index+1,
            "correct_option":df['correct_answer'].values[0],
            "subject":self.subject.lower()
        }
        
        excute_query_without_commit(cursor=cursor,query=query,arguments=args)
        
        
    def add_data(self):
        
        conn,cursor = create_connection()
        
        self.output_df = pd.DataFrame(columns=['qp_code','question_number','question_tag',"correct_answer"])
        
        if 'type' not in self.excel_question_df:
          self.excel_question_df['type'] = 0
          
        baisc_questions = self.excel_question_df.loc[self.excel_question_df['type'] == 2]['Question Tag'].to_list()
        
        normal_question = self.excel_question_df.loc[self.excel_question_df['type'] == 0]['Question Tag'].to_list()
        
        print(len(normal_question))

        random.shuffle(normal_question) 
        
        critical_question = self.excel_question_df.loc[self.excel_question_df['type'] == 1]['Question Tag'].to_list()
        
        print(len(baisc_questions))
        print(len(normal_question))
        print(len(critical_question))

        index = -1
        excute_query_without_commit(query=f"""delete from lapis.lapis_correct_options_detail
        where base_question_paper_code = {self.qp_code}""",cursor=cursor)
        conn.commit()

        for each_question_tag in baisc_questions:
            index += 1 
            self.add_question(each_question_tag=each_question_tag,index=index,conn=conn,cursor=cursor)

        for each_question_tag in normal_question:
            index += 1 
            self.add_question(each_question_tag=each_question_tag,index=index,conn=conn,cursor=cursor)


        if len(critical_question) > 0:
            self.doc.append(NoEscape(r'\vspace{0.5cm}'))
            self.doc.append(NoEscape(r'''
                                    \begin{minipage}{\textwidth}
                                    '''))
            self.doc.append(NoEscape(r' \Centering \Large \textbf{Critical Thinking Questions}'))
            self.doc.append(NoEscape(r'''
                                    \end{minipage}
                                    '''))
            for each_question_tag in critical_question:
                index += 1 
                self.add_question(each_question_tag=each_question_tag,index=index,conn=conn,cursor=cursor)
                
        conn.commit()

        self.output_df.to_csv(f"{self.qp_code}_class_{self.std}_{self.subject}_answerkey.csv",index=False)
        self.doc.change_page_style("bodypage")
    
    
    def add_watermark(self):
        bodypage = PageStyle("bodypage")
        emptypage = PageStyle("emptypage")

        with emptypage.create(Foot("R")):
            emptypage.append(NoEscape(r"""\vspace{0cm}"""))
            emptypage.append(NoEscape(r"Page \thepage"))
            # bodypage.append(NoEscape(r"""\hspace{3cm}"""))

        with emptypage.create(Foot("C")):
            emptypage.append(NoEscape(r"""\vspace{0cm}"""))
            emptypage.append(NoEscape(f"www.learnbasics.fun"))
      
        with emptypage.create(Foot("L")):
            emptypage.append(NoEscape(f"LaPIS - {self.qp_code}"))

        with bodypage.create(Foot("R")):
            bodypage.append(NoEscape(r"""\vspace{0cm}"""))
            bodypage.append(NoEscape(r"Page \thepage"))
            # bodypage.append(NoEscape(r"""\hspace{3cm}"""))

        emptypage.append(pl.Command('renewcommand', arguments=[pl.NoEscape(r'\footrulewidth'),'1pt']))

        with bodypage.create(Foot("C")):
            bodypage.append(NoEscape(r"""\vspace{0cm}"""))
            bodypage.append(NoEscape(f"www.learnbasics.fun"))
      
        with bodypage.create(Foot("L")):
            bodypage.append(NoEscape(f"LaPIS - {self.qp_code}"))

        with bodypage.create(Head("C")):
            bodypage.append(NoEscape(r"Enhanced by Learn Basics"))
                    
        bodypage.append(pl.Command('renewcommand', arguments=[pl.NoEscape(r'\footrulewidth'),'1pt']))
        bodypage.append(pl.Command('renewcommand', arguments=[pl.NoEscape(r'\headrulewidth'),'1pt']))

        self.doc.preamble.append(bodypage)
        self.doc.preamble.append(emptypage)

        self.doc.change_document_style("bodypage")



        self.doc.append(NoEscape(r'''\SetWatermarkLightness{ 0.95 }
        \SetWatermarkText{Learn Basics}
        \SetWatermarkScale{ 0.7 }

        \setlength{\headsep}{0cm}'''))

    def download_images(self):
        client = create_client()
        print(self.folder_name)
        # list_of_images_to_download = client.list_objects(bucket_name='lapis-question-paper-images',prefix=self.folder_name)
        list_of_images_to_download = client.list_objects(bucket_name='lapis-question-paper-images',recursive=True,prefix=self.folder_name)

        for each_file in list_of_images_to_download:
            print(each_file.object_name)
            
    def main(self):
        # self.download_images()
        self.generate_question_paper()
        
    def generate_question_paper(self):
      
        geometry_options = {"top": "1.4cm", "bottom" : "0.5cm", "left" : "1cm", "right" : "1cm"}
        self.doc = Document(geometry_options=geometry_options)
        self.doc.documentclass = Command('documentclass', options=['10pt', 'a4paper'], arguments=['article'])
        self.get_lib()
        self.add_custom_commands()
        self.get_questions()
        self.add_watermark()
        self.add_header()
        self.add_instuctions()
        self.add_data()
        self.doc.generate_tex(f"{self.folder_name}/{self.school_name}_{self.subject}_{self.qp_code}_fianl_1")
        # file_name = f"{self.folder_name}/{self.student_details_dict['roll_number'][str(self.ly_learner_id)]}_{self.student_details_dict['student_name'][str(self.ly_learner_id)]}"
        self.doc.generate_pdf(filepath = f"{self.folder_name}/{self.school_name}_{self.subject}_{self.qp_code}_{self.version}", compiler='pdflatex',clean_tex = False)
        # self.upload_to_minio()
        
    def upload_to_minio(self):
      client = create_client()
      try:
          client.fput_object(bucket_name='lapis',object_name = f"{self.school_name}_{self.subject}_{self.qp_code}_{self.version}.pdf",file_path=f"{self.folder_name}/{self.school_name}_{self.subject}_{self.qp_code}_{self.version}.pdf")
          client.fput_object(bucket_name='lapis',object_name = f"{self.school_name}_{self.subject}_{self.qp_code}_{self.version}.tex",file_path=f"{self.folder_name}/{self.school_name}_{self.subject}_{self.qp_code}_{self.version}.tex")
      except Exception as err:
          print(err)
          raise Exception("failed to move files")

    def get_questions(self):
        query_for_question = f"""SELECT * FROM content.lapis_question_pool
        where question_text is not null"""

        self.question_df = processQuery(query=query_for_question)

        self.total_marks = self.excel_question_df.shape[0]


if __name__ == "__main__":
    qp_code = 659

    folder_name = "C6S"

    std = folder_name[1]

    if folder_name[2] == 'S':
        subject_name = "Science"
    elif folder_name[2] == 'M':
        subject_name = "Math"

    school_name = "PGP International School"
    school_name = "MVM Salem"
    
    excel_df = pd.read_excel(f"Question Selection For Lapis.xlsx",sheet_name=f"MVM - Class {std}")
    
    excel_df['Subject'] = excel_df['Subject'].apply(lambda x:x.strip())

    excel_question_df = excel_df.loc[excel_df['Subject'] == subject_name]
    
    QuestionPaper(qp_code = qp_code,subject=subject_name,folder_name = folder_name,std=std,version = "1.0",excel_question_df=excel_question_df,school_name = school_name).main()
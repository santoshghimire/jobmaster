DROP TABLE IF EXISTS  drushim;

CREATE TABLE drushim(

            Site VARCHAR (255),
            Company VARCHAR (255),
            Company_jobs VARCHAR (255),
            Job_id INTEGER(10) PRIMARY KEY ,
            Job_title VARCHAR (255),
            Job_Description TEXT ,
            Job_Post_Date VARCHAR (20),
            Job_URL TEXT,
            Country_Areas VARCHAR (255),
            Job_categories VARCHAR (255),
            AllJobs_Job_class VARCHAR (255)
) DEFAULT CHARSET = utf8;
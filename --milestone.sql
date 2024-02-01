--milestone
CREATE DATABASE Tournament

GO

CREATE PROC createAllTables 
AS
BEGIN
CREATE TABLE SystemUser (
    username VARCHAR(20) PRIMARY KEY,
    password VARCHAR(20)
)
END

BEGIN
CREATE TABLE Stadium_manager(
    id INT IDENTITY,
    username VARCHAR(20),
    name VARCHAR(20)
    CONSTRAINT FK_manager_username FOREIGN KEY (username) REFERENCES SystemUser(username),
    CONSTRAINT PK_manager PRIMARY KEY (id, username)
)
END

BEGIN 
CREATE TABLE Club_representative (
    id INT IDENTITY,
    username VARCHAR(20),
    name VARCHAR(20),
    CONSTRAINT FK_representative FOREIGN KEY (username) REFERENCES SystemUser(username),
    CONSTRAINT PK_representative PRIMARY KEY (id, username)
)
END

BEGIN 
CREATE TABLE fan(
    National_id INT,
    username VARCHAR (20),
    birth_date DATE,
    phone_number VARCHAR (20),
    name VARCHAR(20),
    Address VARCHAR (20),
    status BIT ,
    CONSTRAINT FK_fan FOREIGN KEY (username) REFERENCES SystemUser (username),
    CONSTRAINT PK_fan PRIMARY KEY (National_id, username)
)
END

BEGIN
CREATE TABLE Sports_association_manager (
    id INT IDENTITY,
    username VARCHAR (20),
    name VARCHAR (20),
    CONSTRAINT FK_association FOREIGN KEY (username) REFERENCES SystemUser (username),
    CONSTRAINT PK_association PRIMARY KEY (id, username)
)
END

BEGIN
CREATE TABLE System_admin (
    id INT IDENTITY,
    username VARCHAR (20),
    name VARCHAR (20),
    CONSTRAINT FK_admin FOREIGN KEY (username) REFERENCES SystemUser (username),
    CONSTRAINT PK_admin PRIMARY KEY (id, username)
)
END

BEGIN
CREATE TABLE Stadium (
    id INT PRIMARY KEY IDENTITY,
    name VARCHAR(20),
    status BIT,
    capacity INT,
    location VARCHAR(20),
    sid INT,
    username VARCHAR(20),
    CONSTRAINT FK_stadium FOREIGN KEY (sid,username) REFERENCES Stadium_manager (id,username)
)
END

BEGIN
CREATE TABLE Club (
    id INT PRIMARY KEY IDENTITY,
    name VARCHAR(20),
    location VARCHAR(20),
    cid INT ,
    username VARCHAR(20),
    CONSTRAINT FK_Club FOREIGN KEY (cid,username) REFERENCES Club_representative (id,username)
)
END

BEGIN
CREATE TABLE Match (
    id INT PRIMARY KEY IDENTITY,
    Start_time TIME,
    End_time TIME,
    stadium_id INT,
    host INT,
    guest INT,
    CONSTRAINT FK_match FOREIGN KEY (stadium_id) REFERENCES Stadium (id),
    CONSTRAINT FK_match1 FOREIGN KEY (host) REFERENCES Club (id),
    CONSTRAINT FK_match2 FOREIGN KEY (guest) REFERENCES Club (id)
)
END

BEGIN
CREATE TABLE Host_request(
    id INT PRIMARY KEY IDENTITY,
    status VARCHAR(20) CHECK(status='unhandled' OR status='accepted' OR status='rejected'),
    match_id INT,
    representative_id INT,
    representative_username VARCHAR(20),
    manager_id INT,
    manager_username VARCHAR(20),
    CONSTRAINT FK_request FOREIGN KEY (representative_id,representative_username) REFERENCES Club_representative (id,username),
    CONSTRAINT FK_request2 FOREIGN KEY (manager_id,manager_username) REFERENCES Stadium_manager (id,username)
)
END

BEGIN
CREATE TABLE Ticket(
id INT PRIMARY KEY IDENTITY,
status BIT,
match_id INT,
fan_id INT, 
fan_username VARCHAR (20),
CONSTRAINT FK_ticket1 FOREIGN KEY (match_id) REFERENCES Match (id),
CONSTRAINT FK_ticket2 FOREIGN KEY (fan_id, fan_username) REFERENCES fan (National_id, username)
)
END

GO

CREATE PROC dropAllTables 
AS 
BEGIN
DROP TABLE Ticket;
DROP TABLE Host_request;
DROP TABLE Match;
DROP TABLE Club;
DROP TABLE Stadium;
DROP TABLE System_admin;
DROP TABLE Sports_Association_Manager;
DROP TABLE fan;
DROP TABLE Club_representative;
DROP TABLE Stadium_manager;
DROP TABLE SystemUser

END
GO
CREATE PROC clearAllTables
AS

BEGIN 

TRUNCATE TABLE Ticket;
TRUNCATE TABLE Host_request;
TRUNCATE TABLE Match;
TRUNCATE TABLE Club;
TRUNCATE TABLE Stadium;
TRUNCATE TABLE System_admin;
TRUNCATE TABLE Sports_Association_Manager;
TRUNCATE TABLE fan;
TRUNCATE TABLE Club_representative;
TRUNCATE TABLE Stadium_manager;
TRUNCATE TABLE SystemUser;

END

GO 


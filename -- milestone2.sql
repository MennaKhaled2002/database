-- milestone2
CREATE PROC createAllTables 
AS
BEGIN
CREATE TABLE SystemUser (
    username VARCHAR(20) PRIMARY KEY,
    password VARCHAR(20)
)
END

BEGIN
CREATE TABLE Club (
    id INT PRIMARY KEY IDENTITY,
    name VARCHAR(20),
    location VARCHAR(20),
)
END

BEGIN
CREATE TABLE Stadium (
    id INT PRIMARY KEY IDENTITY,
    name VARCHAR(20),
    status BIT,
    capacity INT,
    location VARCHAR(20),
   
)
END

BEGIN
CREATE TABLE Stadium_manager(
    id INT IDENTITY,
    username VARCHAR(20),
    name VARCHAR(20),
    stadium INT,
    CONSTRAINT FK_manager_username FOREIGN KEY (username) REFERENCES SystemUser(username) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT PK_manager PRIMARY KEY (id, username) ,
    CONSTRAINT FK_ST FOREIGN KEY (stadium) REFERENCES Stadium(id) ON UPDATE CASCADE ON DELETE CASCADE
)
END

BEGIN 
CREATE TABLE Club_representative (
    id INT IDENTITY,
    username VARCHAR(20),
    name VARCHAR(20),
    cid INT,
    CONSTRAINT FK_representative FOREIGN KEY (username) REFERENCES SystemUser(username) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT PK_representative PRIMARY KEY (id, username),
    CONSTRAINT FK_cid FOREIGN KEY (cid) REFERENCES Club(id) ON UPDATE CASCADE ON DELETE CASCADE
)
END

BEGIN 
CREATE TABLE fan(
    National_id VARCHAR(20),
    username VARCHAR (20),
    birth_date DATE,
    phone_number INT,
    name VARCHAR(20),
    Address VARCHAR (20),
    status BIT ,
    CONSTRAINT FK_fan FOREIGN KEY (username) REFERENCES SystemUser (username) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT PK_fan PRIMARY KEY (National_id, username)
)
END

BEGIN
CREATE TABLE Sports_association_manager (
    id INT IDENTITY,
    username VARCHAR (20),
    name VARCHAR (20),
    CONSTRAINT FK_association FOREIGN KEY (username) REFERENCES SystemUser (username) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT PK_association PRIMARY KEY (id, username)
)
END

BEGIN
CREATE TABLE System_admin (
    id INT IDENTITY,
    username VARCHAR (20),
    name VARCHAR (20),
    CONSTRAINT FK_admin FOREIGN KEY (username) REFERENCES SystemUser (username) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT PK_admin PRIMARY KEY (id, username)
)
END


BEGIN
CREATE TABLE Match (
    id INT PRIMARY KEY IDENTITY,
    Start_time DATETIME,
    End_time DATETIME,
    stadium_id INT,
    host INT,
    guest INT,
    CONSTRAINT FK_match FOREIGN KEY (stadium_id) REFERENCES Stadium (id) ON UPDATE CASCADE ON DELETE SET NULL,
    CONSTRAINT FK_match1 FOREIGN KEY (host) REFERENCES Club (id) ON UPDATE CASCADE ON DELETE CASCADE ,
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
    CONSTRAINT FK_request FOREIGN KEY (representative_id,representative_username) REFERENCES Club_representative (id,username) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT FK_request2 FOREIGN KEY (manager_id,manager_username) REFERENCES Stadium_manager (id,username)
)
END

BEGIN
CREATE TABLE Ticket(
    id INT PRIMARY KEY IDENTITY,
    status BIT,
    match_id INT,
    CONSTRAINT FK_ticket1 FOREIGN KEY (match_id) REFERENCES Match (id) 
    ON UPDATE CASCADE  ON DELETE CASCADE,
    
)
END
CREATE TABLE Ticket_Buying(
    fan_National_id VARCHAR(20),
    username VARCHAR(20),
    ticket_id INT,
    CONSTRAINT Ticket_Buying1 FOREIGN KEY (fan_National_id,username) REFERENCES fan(National_id,username) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT Ticket_Buying2 FOREIGN KEY (ticket_id) REFERENCES Ticket(id) ON UPDATE CASCADE ON DELETE CASCADE
)
GO

CREATE PROC dropAllTables 
AS 
BEGIN
DROP TABLE Ticket_Buying
DROP TABLE Sports_Association_Manager;
DROP TABLE Ticket;
DROP TABLE Host_request;
DROP TABLE Match;
DROP TABLE Club_representative;
DROP TABLE Club;
DROP TABLE Stadium_manager;
DROP TABLE Stadium;
DROP TABLE System_admin;
DROP TABLE fan;
DROP TABLE SystemUser

END
GO


CREATE PROC clearAllTables
AS

BEGIN 
DELETE FROM  Host_request;
DELETE FROM  Ticket_Buying
DELETE FROM  Ticket;
DELETE FROM  Match;
DELETE FROM  Club_representative;
DELETE FROM Club;
DELETE FROM  Stadium_manager;
DELETE FROM Stadium;
DELETE FROM  System_admin;
DELETE FROM fan;
DELETE FROM Sports_Association_Manager;
DELETE FROM  SystemUser

END
GO


CREATE VIEW allAssocManagers 
AS
SELECT S.username,U.password, S.name
FROM Sports_association_manager S INNER JOIN SystemUser U ON S.username=U.username


GO


CREATE VIEW allClubRepresentatives
AS
SELECT C.username,C.name,Cl.name AS Club,U.password
FROM Club_representative C INNER JOIN Club Cl ON C.cid=CL.id INNER JOIN SystemUser U 
ON C.username=U.username


GO

CREATE VIEW allStadiumManagers
AS
SELECT M.username, M.name,S.name AS Stadium
FROM Stadium_manager M INNER JOIN Stadium S ON M.stadium=S.id INNER JOIN SystemUser U 
ON U.username=M.username

GO

CREATE VIEW allFans
AS
SELECT f.username,U.password,f.name, f.National_id, f.birth_date, f.status
FROM fan f INNER JOIN SystemUser U ON f.username=U.username


GO
CREATE VIEW allMatches
AS
SELECT C1.name AS Host_club ,C2.name AS guest_club , M.Start_time
FROM Match M INNER JOIN Club C1 ON M.host=C1.id INNER JOIN Club C2 ON M.guest=C2.id

GO

CREATE VIEW allTickets
AS
SELECT C1.name AS Host_club ,C2.name AS Guest_club ,S.name, M.Start_time
FROM  Match M INNER JOIN Stadium S ON M.stadium_id=S.id INNER JOIN Ticket T ON T.match_id=M.id 
INNER JOIN Club C1 ON M.host=C1.id INNER JOIN Club C2 ON M.guest=C2.id

GO

CREATE VIEW allCLubs
AS
SELECT C.name, C.location
FROM Club C

GO
CREATE VIEW allStadiums
AS 
SELECT S.name,S.location, S.capacity, S.status
FROM Stadium S

GO
CREATE VIEW allRequests
AS 
SELECT C.name AS representative, S.name AS manager, H.status
FROM Host_request H INNER JOIN Club_representative C ON C.id= H.representative_id 
INNER JOIN Stadium_manager S ON S.id=H.manager_id INNER JOIN SystemUser U1 ON C.username=U1.username
INNER JOIN SystemUser U2 ON S.username=U2.username


GO

CREATE PROC addAssociationManager @name VARCHAR(20),@username VARCHAR(20),
@password VARCHAR(20)
AS
BEGIN
INSERT INTO SystemUser VALUES(@username,@password)
INSERT INTO Sports_association_manager VALUES(@username,@name)
END

GO


CREATE PROC addNewMatch @Host_club VARCHAR(20), @Guest_club VARCHAR(20),@Start DATETIME
,@End DATETIME
AS
BEGIN 
DECLARE @C1 INT
DECLARE @C2 INT
DECLARE @S INT


SELECT @C1=C.id
FROM Club C
WHERE C.name=@Host_club

SELECT @C2=C.id
FROM Club C
WHERE C.name=@Guest_club

SELECT @S=M.stadium_id -- stadium id already assigned accept request
FROM Match M
WHERE @C1=M.host AND @C2=M.guest

INSERT INTO Match VALUES(@Start,@End,@S,@C1,@C2)

END


GO

CREATE VIEW clubsWithNoMatches
AS
SELECT C.name
FROM Club C
EXCEPT
SELECT C1.name
FROM Club C1 INNER JOIN Match M ON C1.id=M.host OR C1.id=M.guest


GO

CREATE PROC deleteMatch @first_club VARCHAR(20),@second_club VARCHAR(20) 
AS
DECLARE @C1 INT
DECLARE @C2 INT


SELECT @C1=C.id
FROM Club C
WHERE C.name=@First_club

SELECT @C2=C.id
FROM Club C
WHERE C.name=@Second_club


DELETE FROM Match WHERE host=@C1 AND guest=@C2


GO


CREATE PROC addClub @name VARCHAR(20),@Loc VARCHAR(20)
AS
INSERT INTO Club VALUES(@name,@Loc)

GO

CREATE PROC addTicket @host VARCHAR(20), @competitor VARCHAR(20),@start DATETIME
AS
DECLARE @h INT
DECLARE @C INT
DECLARE @match INT

SELECT @h=C.id
FROM Club C
WHERE C.name=@host

SELECT @C=C.id
FROM Club C
WHERE C.name=@competitor

SELECT @match=M.id
FROM Match M
WHERE M.host=@h AND M.guest=@C AND M.Start_time=@start

INSERT INTO Ticket (status,match_id) VALUES (1,@match) 

GO

CREATE PROC deleteClub @name VARCHAR(20)
AS
DECLARE @ID INT

SELECT @ID=C.id
FROM Club C  
WHERE C.name=@name

DELETE FROM Match WHERE host=@ID OR guest=@ID 

DELETE FROM Club WHERE name=@name

GO

CREATE PROC addStadium @name VARCHAR(20),@Loc VARCHAR(20),@capacity INT 
AS
INSERT INTO Stadium VALUES (@name,1,@capacity,@Loc)

GO

CREATE PROC deleteStadium @name VARCHAR(20)
AS
DELETE FROM Stadium WHERE name=@name

GO

CREATE PROC blockFan @nat VARCHAR(20)
AS
UPDATE fan 
SET status=0
WHERE National_id=@nat

GO

CREATE PROC unblockFan @nat VARCHAR(20)
AS
UPDATE fan 
SET status=1
WHERE National_id=@nat

GO

CREATE PROC addRepresentative @name VARCHAR(20),@club VARCHAR(20),@username VARCHAR(20),
@password VARCHAR(20)
AS
INSERT INTO SystemUser VALUES (@username,@password)
DECLARE @C INT
SELECT @C=Cl.id
FROM Club Cl
WHERE Cl.name=@club
INSERT INTO Club_representative VALUES (@username,@name,@C)


GO
-- HOW TO HANDLE STATUS?
CREATE FUNCTION viewAvailableStadiumsOn (@time DATETIME)
RETURNS TABLE
AS
RETURN SELECT S.name,S.location,S.capacity FROM Stadium S INNER JOIN Match M ON S.id=M.stadium_id
WHERE  S.status=1 AND M.Start_time <>@time

GO

GO

CREATE PROC addHostRequest @club VARCHAR(20), @Stadium VARCHAR(20),@start DATETIME
AS
DECLARE @CID INT
SELECT @CID=id
FROM Club WHERE name=@club
DECLARE @rep_id INT
DECLARE @rep_username VARCHAR(20) 

SELECT @rep_id=CR.id,@rep_username=CR.username
FROM Club_representative CR 
WHERE CR.cid=@CID

DECLARE @SID INT
SELECT @SID=S.id
FROM Stadium S 
WHERE S.name=@Stadium

DECLARE @man_id INT
DECLARE @man_username VARCHAR(20) 
SELECT @man_id=S.id,@man_username=S.username
FROM Stadium_manager S
WHERE S.stadium=@SID

DECLARE @match INT
SELECT @match=M.id
FROM Match M 
WHERE M.host=@CID AND M.Start_time=@start 

INSERT INTO Host_request VALUES('unhandled',@match,@rep_id,@rep_username,@man_id,@man_username) 

GO


CREATE FUNCTION allUnassignedMatches (@host VARCHAR(20))
RETURNS TABLE --WHEN DO WE ASSIGN CLUBS??
AS
RETURN SELECT C2.name,M.Start_time FROM Match M RIGHT JOIN Club C1 
ON M.host=C1.id RIGHT JOIN Club C2 ON M.guest=C2.id
WHERE C1.name=@host AND M.stadium_id IS NULL


GO

CREATE PROC addStadiumManager @name VARCHAR(20), @stadium VARCHAR(20),
@username VARCHAR(20),@password VARCHAR(20)
AS
INSERT INTO SystemUser VALUES(@username,@password)

DECLARE @SID INT
SELECT @SID=S.id
FROM Stadium S 
WHERE S.name=@Stadium

INSERT INTO Stadium_manager VALUES(@username,@name,@SID)


GO
CREATE FUNCTION allPendingRequests (@manager VARCHAR(20))
RETURNS TABLE
RETURN SELECT CR.name AS representative, C.name AS Guest, M.Start_time
FROM Host_request HR INNER JOIN Stadium_manager SM ON HR.manager_id=SM.id INNER JOIN
Club_representative CR ON HR.representative_id=CR.id INNER JOIN 
Match M ON HR.match_id=M.id INNER JOIN Club C ON M.guest=C.id
WHERE SM.username=@manager AND HR.status='unhandled'

GO


CREATE PROC acceptRequest @stadium_manager VARCHAR(20),@Host VARCHAR(20),@guest VARCHAR(20),
@Start DATETIME
AS
DECLARE @match INT
DECLARE @h INT
DECLARE @g INT
DECLARE @manager INT
DECLARE @S INT
DECLARE @capacity INT

SELECT @h=C.id
FROM Club C WHERE C.name=@Host

SELECT @g=C.id
FROM Club C WHERE C.name=@guest

SELECT @match=M.id
FROM Match M 
WHERE M.host=@h AND m.guest=@g AND M.Start_time=@start 

SELECT @manager=S.id
FROM Stadium_manager S
WHERE S.username= @Stadium_manager

UPDATE Host_request
SET status='accepted'
WHERE match_id=@match AND manager_id=@manager

SELECT @S=S.id,@capacity=S.capacity
FROM Stadium S INNER JOIN Stadium_manager M ON S.id=M.stadium

UPDATE Match
SET stadium_id=@S
WHERE host=@h AND guest=@g AND Start_time=@start 

DECLARE @i INT=0  
WHILE @i<@capacity
BEGIN
EXEC addTicket @host,@guest,@Start
SET @i=@i+1
END

GO
CREATE PROC rejectRequest @stadium_manager VARCHAR(20),@Host VARCHAR(20),@guest VARCHAR(20),
@Start DATETIME
AS
DECLARE @match INT
DECLARE @h INT
DECLARE @g INT
DECLARE @manager INT

SELECT @h=C.id
FROM Club C WHERE C.name=@Host

SELECT @g=C.id
FROM Club C WHERE C.name=@guest

SELECT @match=M.id
FROM Match M 
WHERE M.host=@h AND m.guest=@g AND M.Start_time=@start 

SELECT @manager=S.id
FROM Stadium_manager S
WHERE S.username= @Stadium_manager

UPDATE Host_request
SET status='rejected'
WHERE match_id=@match AND manager_id=@manager


GO

CREATE PROC addFan @name VARCHAR(20), @National_id VARCHAR(20), @birth_date VARCHAR(20),
@Address VARCHAR(20), @phone INT, @username VARCHAR(20), @password VARCHAR(20) 
AS
INSERT INTO SystemUser VALUES(@username,@password)
INSERT INTO fan VALUES (@National_id,@username,@birth_date,@phone,@name,@Address,1)


GO
CREATE FUNCTION upcomingMatchesOfClub (@club VARCHAR(20))
RETURNS TABLE
AS
RETURN SELECT C1.name AS club1, c2.name AS club2, M.Start_time,S.name AS stadium
FROM Match M INNER JOIN Club C1 ON M.host=C1.id INNER JOIN Club C2 ON M.guest=C2.id INNER JOIN
Stadium S ON M.stadium_id=S.id 
WHERE (C1.name=@club OR C2.name=@club) AND GETDATE()<M.Start_time


GO
CREATE FUNCTION availableMatchesToAttend(@date DATETIME)
RETURNS TABLE
AS
RETURN SELECT C1.name AS host, C2.name AS guest,M.Start_time, S.name
FROM Match M INNER JOIN Host_request H ON M.id=H.match_id INNER JOIN Club C1 ON M.host=C1.id
INNER JOIN Club C2 ON M.guest=C2.id INNER JOIN Stadium S ON M.stadium_id=S.id
WHERE M.Start_time>@date AND H.status='accepted' 



GO
CREATE PROC purchaseTicket @National_id VARCHAR(20),@Host VARCHAR(20),@guest VARCHAR(20),
@Start DATETIME
AS
DECLARE @match INT
DECLARE @h INT
DECLARE @g INT
DECLARE @U VARCHAR(20)
DECLARE @Ticket INT

SELECT @U=f.username
FROM fan f WHERE f.National_id=@National_id

SELECT @h=C.id
FROM Club C WHERE C.name=@Host

SELECT @g=C.id
FROM Club C WHERE C.name=@guest

SELECT @match=M.id
FROM Match M 
WHERE M.host=@h AND m.guest=@g AND M.Start_time=@start 

SELECT TOP 1 @Ticket= T.id 
FROM Ticket T
WHERE T.status=1 AND T.match_id=@match

UPDATE Ticket
SET status=0
WHERE match_id=@match AND id=@Ticket

INSERT INTO Ticket_Buying VALUES (@National_id,@U,@Ticket)



GO
CREATE PROC updateMatchHost @host VARCHAR(20),@guest VARCHAR(20),@date DATETIME
AS
DECLARE @h INT
DECLARE @g INT

SELECT @h=C.id
FROM Club C WHERE C.name=@host

SELECT @g=C.id
FROM Club C WHERE C.name=@guest

UPDATE Match
SET Match.host=@g, Match.guest=@h
WHERE Match.host=@h AND Match.guest=@g AND Match.Start_time=@date


GO
CREATE PROC deleteMatchesOnStadium @stadium VARCHAR(20)
AS
DECLARE @S INT
SELECT @S=id
FROM Stadium WHERE name=@stadium

DELETE FROM Match
WHERE GETDATE()< Start_time AND stadium_id=@S


GO

CREATE VIEW matchesPerTeam
AS
SELECT C.name,COUNT(*) AS matchNumber
FROM Match M INNER JOIN Club C ON M.host=C.id OR M.guest=C.id
GROUP BY C.name


GO


CREATE VIEW clubsNeverMatched
AS
SELECT C1.name AS First_club,C2.name AS Second_club
FROM Club C1 ,Club c2
WHERE C1.name<>C2.name AND C1.name>C2.name
EXCEPT
SELECT C3.name AS First_club,C4.name AS Second_club
FROM Match M INNER JOIN Club C3 ON M.host=C3.id INNER JOIN Club C4 ON M.guest=C4.id
EXCEPT
SELECT C6.name AS First_club,C5.name AS Second_club
FROM Match M INNER JOIN Club C5 ON M.host=C5.id INNER JOIN Club C6 ON M.guest=C6.id



GO

CREATE FUNCTION clubsNeverPlayed (@club VARCHAR(20))
RETURNS TABLE
AS
RETURN SELECT C.name
FROM Club C
WHERE C.name <> @club AND C.name
NOT IN (SELECT C3.name FROM Club C1 INNER JOIN Match M ON C1.id=M.host 
INNER JOIN Club C3 ON M.guest=C3.id WHERE C1.name=@club ) AND 
C.name NOT IN (SELECT C4.name FROM Club C2 INNER JOIN Match M1 ON C2.id=M1.guest
INNER JOIN Club C4 ON C4.id=M1.host WHERE C2.name=@club ) 

GO


CREATE FUNCTION matchesRankedByAttendance ()
RETURNS TABLE
AS
RETURN SELECT C1.name AS Host,C2.name AS Guest,COUNT(*) AS NumOfTickets
FROM Match M INNER JOIN Club C1 ON M.host=C1.id INNER JOIN Club C2 ON C2.id=M.guest 
INNER JOIN Ticket T ON M.id=T.match_id
WHERE T.status=0
GROUP BY C1.name,C2.name
ORDER BY COUNT(*) DESC
OFFSET 0 ROWS       

GO
CREATE FUNCTION matchWithHighestAttendance ()
RETURNS TABLE
AS
RETURN SELECT TOP 1 WITH TIES D.Host,D.Guest,D.NumOfTickets  
FROM dbo.matchesRankedByAttendance() D
ORDER BY D.NumOfTickets DESC



GO
CREATE FUNCTION requestsFromClub (@stadium VARCHAR(20),@club VARCHAR(20))
RETURNS TABLE 
AS
RETURN SELECT C1.name AS HOST,C2.name AS GUEST 
FROM Match M INNER JOIN Host_request H ON H.match_id=M.id INNER JOIN Club C1
ON C1.id =M.host INNER JOIN Club C2 ON M.guest=C2.id INNER JOIN Stadium S ON M.stadium_id=S.id
WHERE S.name=@stadium AND C1.name=@club 


GO
CREATE PROC dropAllProceduresFunctionsViews
AS
DROP PROC createAllTables
DROP PROC clearAllTables
DROP PROC dropAllTables
DROP VIEW allAssocManagers
DROP VIEW allClubRepresentatives
DROP VIEW allStadiumManagers
DROP VIEW allFans
DROP VIEW allMatches
DROP VIEW allTickets
DROP VIEW allCLubs
DROP VIEW allStadiums
DROP VIEW allRequests
DROP VIEW clubsWithNoMatches
DROP VIEW matchesPerTeam
DROP VIEW clubsNeverMatched
DROP PROC addAssociationManager
DROP PROC addNewMatch
DROP PROC deleteMatch
DROP PROC deleteMatchesOnStadium
DROP PROC addClub
DROP PROC addTicket
DROP PROC deleteClub
DROP PROC addStadium
DROP PROC deleteStadium
DROP PROC blockFan
DROP PROC unblockFan
DROP PROC addRepresentative
DROP PROC addHostRequest
DROP FUNCTION viewAvailableStadiumsOn
DROP FUNCTION allUnassignedMatches
DROP PROC addStadiumManager
DROP FUNCTION allPendingRequests
DROP PROC acceptRequest
DROP PROC rejectRequest
DROP PROC addFan
DROP FUNCTION upcomingMatchesOfClub
DROP FUNCTION availableMatchesToAttend
DROP PROC purchaseTicket
DROP PROC updateMatchHost
DROP FUNCTION clubsNeverPlayed
DROP FUNCTION matchWithHighestAttendance
DROP FUNCTION matchesRankedByAttendance
DROP FUNCTION requestsFromClub 

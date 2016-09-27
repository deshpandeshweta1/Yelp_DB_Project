
CREATE TABLE maincategory( maincategory_id INTEGER NOT NULL PRIMARY KEY, maincategory_name VARCHAR(50) NOT NULL);


CREATE TABLE business (
  type varchar(10) NOT NULL,
  business_id varchar(100) PRIMARY KEY,
  name varchar(100) NOT NULL,
  full_address varchar(255) NULL,
  city varchar(50) NULL,
  state varchar(50) NULL,
  star number(3,2),
  review_count INTEGER NULL);
	
 
 CREATE TABLE category( 
 category_id INTEGER NOT NULL PRIMARY KEY,
  business_id VARCHAR(100) NOT NULL,
  maincategory_name VARCHAR(50) NOT NULL,
  subcategory_name VARCHAR(50),
  FOREIGN KEY (business_id) REFERENCES business(business_id));
	
	
CREATE TABLE days(day_id number(2) NOT NULL,day CHAR(20), PRIMARY KEY(day_id));
	
	
CREATE TABLE yelp_user
( user_id VARCHAR(100) NOT NULL,
  name VARCHAR(50) NOT NULL,
  yelping_since varchar(20),
  review_count INTEGER,
  fans INTEGER,
  average_stars number(3,2),
  type varchar(10) NOT NULL,
  Number_of_friends INTEGER,
  CONSTRAINT yelp_user_pk PRIMARY KEY (user_id)
);	
	
CREATE TABLE review(
review_id VARCHAR(100)NOT NULL,
business_id VARCHAR(100),
user_id VARCHAR(100),
publish_date date,
star NUMBER(2),
vote_funny INTEGER,
vote_useful INTEGER,
vote_cool INTEGER,
text CLOB,
PRIMARY KEY(review_id),
foreign key (user_id) references yelp_user(user_id) on delete cascade,
foreign key (business_id) references business(business_id) on delete cascade
);	


	CREATE TABLE Checkin(
	business_id VARCHAR(100)NOT NULL,
	startHour number(2) NOT NULL,
	day_id number(2) NOT NULL,
	numberOfCheckin number(3),
	PRIMARY KEY(business_id,day_id,startHour),
    foreign key (business_id) references business(business_id) on delete cascade,
	FOREIGN KEY (day_id) REFERENCES days(day_id)
);

CREATE INDEX reviewBusiness
on REVIEW (business_id);

CREATE INDEX reviewUser
on REVIEW (user_id);


INSERT INTO maincategory VALUES(1,'Active Life');
INSERT INTO maincategory VALUES(2,'Arts '||'&'||' Entertainment');
INSERT INTO maincategory VALUES(3,'Automotive');
INSERT INTO maincategory VALUES(4,'Car Rental');
INSERT INTO maincategory VALUES(5,'Cafes');
INSERT INTO maincategory VALUES(6,'Beauty '||'&'||' Spas');
INSERT INTO maincategory VALUES(7,'Convenience Stores');
INSERT INTO maincategory VALUES(8,'Dentists');
INSERT INTO maincategory VALUES(9,'Doctors');
INSERT INTO maincategory VALUES(10,'Drugstores');
INSERT INTO maincategory VALUES(11,'Department Stores');
INSERT INTO maincategory VALUES(12,'Education');
INSERT INTO maincategory VALUES(13,'Event Planning '||'&'||' Services');
INSERT INTO maincategory VALUES(14,'Flowers '||'&'||' Gifts');
INSERT INTO maincategory VALUES(15,'Food');
INSERT INTO maincategory VALUES(16,'Health '||'&'||' Medical');
INSERT INTO maincategory VALUES(17,'Home Services');
INSERT INTO maincategory VALUES(18,'Home '||'&'||' Garden');
INSERT INTO maincategory VALUES(19,'Hospitals');
INSERT INTO maincategory VALUES(20,'Hotels '||'&'||' Travel');
INSERT INTO maincategory VALUES(21,'Hardware Stores');
INSERT INTO maincategory VALUES(22,'Grocery');
INSERT INTO maincategory VALUES(23,'Medical Centers');
INSERT INTO maincategory VALUES(24,'Nurseries '||'&'||' Gardening');
INSERT INTO maincategory VALUES(25,'Nightlife');
INSERT INTO maincategory VALUES(26,'Restaurants');
INSERT INTO maincategory VALUES(27,'Shopping');
INSERT INTO maincategory VALUES(28,'Transportation');
	

INSERT INTO days VALUES('0','SUN');
INSERT INTO days VALUES('1','MON');
INSERT INTO days VALUES('2','TUE');
INSERT INTO days VALUES('3','WED');
INSERT INTO days VALUES('4','THUR');
INSERT INTO days VALUES('5','FRI');
INSERT INTO days VALUES('6','SAT');
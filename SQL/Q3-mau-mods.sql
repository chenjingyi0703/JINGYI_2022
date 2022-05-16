--****PLEASE ENTER YOUR DETAILS BELOW****
--Q3-mau-mods.sql
--Student ID:31314295
--Student Name:JINGYI CHEN
--Tutorial No:01

/* Comments for your marker:


(ii) Suppose Mau records these three fields in sale instead of in Artist and Gallery table 
(iii) Because of data redundancy, we did not put the artwork title in the table, which can be found by artist code and artwork id

*/


/*
3(i) Changes to live database 1
*/
--PLEASE PLACE REQUIRED SQL STATEMENTS FOR THIS PART HERE


ALTER TABLE CUSTOMER
	ADD (CUSTOMER_BOUGHT_TIMES NUMBER(4));

COMMENT ON COLUMN CUSTOMER.CUSTOMER_BOUGHT_TIMES IS
    'the total number of times each customer has bought an artwork';


UPDATE CUSTOMER
	SET CUSTOMER_BOUGHT_TIMES = (
		SELECT 
			count(*)
		from sale
		where customer_id = CUSTOMER.customer_id
	);

commit;

/*
3(ii) Changes to live database 2
*/
--PLEASE PLACE REQUIRED SQL STATEMENTS FOR THIS PART HERE

ALTER TABLE SALE
	ADD (MAU_COMMISSION NUMBER(12,2));

COMMENT ON COLUMN CUSTOMER.CUSTOMER_BOUGHT_TIMES IS
    'the commission in dollars that should be paid to MAU';

ALTER TABLE SALE
	ADD (GALLERY_COMMISSION NUMBER(12,2));

COMMENT ON COLUMN SALE.GALLERY_COMMISSION IS
    'the commission in dollars that should be paid to GALLERY';

ALTER TABLE SALE
	ADD (artist_payment NUMBER(12,2));

COMMENT ON COLUMN SALE.artist_payment IS
	'the actual payment in dollars that should be made to the artist';


update SALE
	set 
		MAU_COMMISSION = round(sale_price * 0.2,2);
commit;


update sale
	set
		GALLERY_COMMISSION = round(sale_price * 0.01 * 
			(
				select 
					gallery_sale_percent
				from 
					sale s 
				join aw_display d
					ON s.aw_display_id = d.aw_display_id
				join gallery g
					ON d.gallery_id = g.gallery_id
				where 
					sale.aw_display_id = d.aw_display_id
						and
					d.gallery_id = g.gallery_id
				),2);

commit;

update sale
	set 
	artist_payment = sale_price - MAU_COMMISSION - GALLERY_COMMISSION;

    
/*
3(iii) Changes to live database 3
*/
--PLEASE PLACE REQUIRED SQL STATEMENTS FOR THIS PART HERE

DROP TABLE exhibition CASCADE CONSTRAINTS;

create table exhibition(
	exhibition_code NUMBER(4) NOT NULL,
    artist_code NUMBER(4) NOT NULL,
	artwork_no NUMBER(4) NOT NULL,
	exhibition_name VARCHAR2(50) NOT NULL,
	exhibition_theme VARCHAR2(50) NOT NULL,
	exhibition_artwork_numbers NUMBER(4) NOT NULL,
--	artwork_title VARCHAR2(100) NOT NULL,
	gallery_id NUMBER(3) NOT NULL

);

COMMENT ON COLUMN exhibition.exhibition_code IS
'Code/Identifier for exhibition';
COMMENT ON COLUMN exhibition.exhibition_name IS
'Name of exhibition';
COMMENT ON COLUMN exhibition.exhibition_theme IS
'Theme of exhibition';
COMMENT ON COLUMN exhibition.exhibition_artwork_numbers IS
'Numbers of artwork in exhibition';
COMMENT ON COLUMN artwork.artist_code IS
'Code/Identifier for artist';
COMMENT ON COLUMN exhibition.artwork_no IS
'id of artwork in exhibition';
--COMMENT ON COLUMN exhibition.artwork_title IS
--'Title of artwork, note a particular artist must use a unique title for each of their artworks';
COMMENT ON COLUMN exhibition.gallery_id IS
'Identifier for Gallery';


ALTER TABLE exhibition ADD CONSTRAINT exhibition_pk PRIMARY KEY ( exhibition_code,artist_code,artwork_no );

ALTER TABLE exhibition
    ADD CONSTRAINT exhibition_artwork FOREIGN KEY ( artwork_no,artist_code  )
        REFERENCES artwork ( artwork_no,artist_code  );

ALTER TABLE exhibition
    ADD CONSTRAINT exhibition_gallery FOREIGN KEY ( gallery_id )
        REFERENCES gallery ( gallery_id );

--ALTER TABLE exhibition
--    ADD CONSTRAINT exhibition_artwork FOREIGN KEY ( artist_code )
--        REFERENCES artwork ( artist_code );

ALTER TABLE exhibition
    ADD CONSTRAINT chk_exhibition CHECK ( exhibition_theme IN ( 'A', 'M', 'O' ) );


INSERT INTO exhibition VALUES(1,10,1,'TEAM WANG','A',15,5);
INSERT INTO exhibition VALUES(1,10,2,'TEAM WANG','A',15,5);






SELECT
JSON_OBJECT (
 '_id' VALUE studid, 
 'name' VALUE studfname || ' ' || studlname,
 'contactInfo' VALUE JSON_OBJECT (
		'address' VALUE studaddress,
		'phone' VALUE rtrim(studphone),
		'email' VALUE studemail
),
'dob' VALUE to_char(studdob,'dd-mm-yyyy'),
'enrolmentInfo' VALUE JSON_ARRAYAGG(
	JSON_OBJECT(
		'unitcode' VALUE unitcode,
        'unitname' VALUE unitname,
		'year' VALUE to_char(ofyear, 'yyyy'),
		'semester' VALUE semester,
		'mark' VALUE mark,
		'grade' VALUE grade
	)
) 
	FORMAT JSON )
|| ','
FROM
	uni.student
NATURAL JOIN uni.enrolment 
NATURAL JOIN uni.unit
GROUP BY
	studid,
	studfname,
	studlname,
	studaddress,
	studphone,
	studemail
	studdob;
ORDER BY
studid;









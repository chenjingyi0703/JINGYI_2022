--****PLEASE ENTER YOUR DETAILS BELOW****
--Q1b-mau-dm.sql
--Student ID:31314295
--Student Name:JINGYI CHEN
--Tutorial No:01

/* Comments for your marker:

To allow tracking the status of an artwork, MAU would like to have recorded ：
1）the status of each artwork at the current point in time
2）its full status history

Since we want to record the entire status history of the work, when the status of the work changes, 
we cannot use UPDATE and need to insert a new record

● W - in MAU storage at the MAU central warehouse
● T - in transit (being shipped to/from a gallery), include to/from which gallery id
● G - located at the gallery, include gallery id
● S - sold, or
● R - returned to the artist

For gallery id status table, due to the T and G state made it clear that need gallery id, so that when the state is G and T, 
we need to insert the gallery id field, but other state gallery id is null, because whether it's in the warehouse or sold, 
or is returned to the author, can assume that time have nothing to do with gallery, so there is no gallery id

*/


/*
1b(i) Create a sequence 
*/
--PLEASE PLACE REQUIRED SQL STATEMENT(S) FOR THIS PART HERE

create sequence aw_display_seq start with 300 increment by 1;

create sequence sale_seq start with 300 increment by 1;

create sequence aw_status_seq start with 300 increment by 1;

commit;

/*
1b(ii) Take the necessary steps in the database to record data.
*/
--PLEASE PLACE REQUIRED SQL STATEMENT(S) FOR THIS PART HERE

insert into artwork values(17,1,'Saint Catherine of Siena',500000,to_date('22-Oct-2020 10:00','dd-Mon-yyyy HH24:Mi'));

insert into AW_STATUS values(aw_status_seq.nextval,17,1,sysdate,'W',null);

commit;
/*
1b(iii) Take the necessary steps in the database to record changes. 
*/
--PLEASE PLACE REQUIRED SQL STATEMENT(S) FOR THIS PART HERE
-- (a)
--update AW_STATUS 
--	set aws_action = 'T', 
--	    aws_date_time = to_date('22-Oct-2020 11:00','dd-Mon-yyyy HH24:Mi'),
--		gallery_id = (select gallery_id from gallery where gallery_phone = '0413432569')
-- where artist_code = 17 and artwork_no = 1;

insert into AW_STATUS values(aw_status_seq.nextval,17,1,to_date('22-Oct-2020 11:00','dd-Mon-yyyy HH24:Mi'),'T',
	(select gallery_id from gallery where gallery_phone = '0413432569'));


commit;
-- (b)
-- update AW_STATUS 
-- 	set aws_action = 'G', 
--		aws_date_time = to_date('22-Oct-2020 14:15','dd-Mon-yyyy HH24:Mi')
-- where artist_code = 17 and artwork_no = 1;


insert into AW_STATUS values(aw_status_seq.nextval,17,1,to_date('22-Oct-2020 14:15','dd-Mon-yyyy HH24:Mi'),'G',
	(select gallery_id from gallery where gallery_phone = '0413432569'));

commit;


-- (c)
insert into aw_display values 
	(aw_display_seq.nextval,
		17,
		1,
		to_date('22-Oct-2020','dd-Mon-yyyy'),
		to_date('2-Nov-2020','dd-Mon-yyyy'),
		(select gallery_id from gallery where gallery_phone = '0413432569')
	);

commit;

/*
1b(iv) Take the necessary steps in the database to record changes. 
*/
--PLEASE PLACE REQUIRED SQL STATEMENT(S) FOR THIS PART HERE
insert into sale values (sale_seq.nextval,to_date('27-Oct-2020 14:30','dd-Mon-yyyy HH24:Mi'),850000,1,aw_display_seq.currval);


commit;







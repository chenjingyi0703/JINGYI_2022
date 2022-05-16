--****PLEASE ENTER YOUR DETAILS BELOW****
--Q1a-mau-insert.sql
--Student ID:31314295
--Student Name:JINGYI CHEN
--Tutorial No:01

/* Comments for your marker:
 
1. To allow tracking the status of an artwork, MAU would like to have recorded ：
1）the status of each artwork at the current point in time
2）its full status history

2. Since we want to record the entire status history of the work, when the status of the work changes, 
we cannot use UPDATE and need to insert a new record

● W - in MAU storage at the MAU central warehouse
● T - in transit (being shipped to/from a gallery), include to/from which gallery id
● G - located at the gallery, include gallery id
● S - sold, or
● R - returned to the artist

3. For gallery id status table, due to the T,G and S state made it clear that need gallery id, so that when the state is G,T and S, 
we need to insert the gallery id field, but other state gallery id is null, because whether it's in the warehouse
or is returned to the author, can assume that time have nothing to do with gallery, so there is no gallery id

4. For T state, since two gallery ids cannot be recorded at the one field, 
Let's assume that when you send out from the exhibit, that is the send out id, and when you send in, that is the send in ID

5. Assuming that the exhibition hall and warehouse are close together, some can be delivered on the same day

*/


/*
1(a) Load selected tables with your own additional test data
*/
--PLEASE PLACE REQUIRED SQL STATEMENT(S) FOR THIS PART HERE

insert into artwork values (1,1,'CAT',500,to_date('2-Jun-2019','dd-Mon-yyyy'));
insert into artwork values (2,1,'RIVER',1500,to_date('4-Sep-2019','dd-Mon-yyyy'));
insert into artwork values (3,2,'SUN',2500,to_date('5-Aug-2019','dd-Mon-yyyy'));
insert into artwork values (4,1,'SAD',3500,to_date('6-Jun-2019','dd-Mon-yyyy'));
insert into artwork values (5,1,'HAPPY',4500,to_date('12-Oct-2019','dd-Mon-yyyy'));
insert into artwork values (6,3,'BEER',5500,to_date('22-Oct-2019','dd-Mon-yyyy'));
insert into artwork values (7,1,'LAKER',6500,to_date('12-Jun-2019','dd-Mon-yyyy'));
insert into artwork values (8,1,'THUNDER',16500,to_date('12-Jun-2019','dd-Mon-yyyy'));
insert into artwork values (9,1,'MAN',26500,to_date('15-Oct-2019','dd-Mon-yyyy'));
insert into artwork values (10,1,'WOMAN',36500,to_date('3-Jun-2020','dd-Mon-yyyy'));
insert into artwork values (11,1,'SEX',30000,to_date('11-Jun-2020','dd-Mon-yyyy'));
insert into artwork values (10,2,'VICE',36500,to_date('2-Jun-2020','dd-Mon-yyyy'));
insert into artwork values (10,3,'SUGER',123500,to_date('1-Jun-2020','dd-Mon-yyyy'));
insert into artwork values (10,4,'DIE',333500,to_date('10-Jun-2020','dd-Mon-yyyy'));
insert into artwork values (10,5,'RICE',3453500,to_date('1-Feb-2020','dd-Mon-yyyy'));

commit;

insert into AW_DISPLAY values(1,1,1,to_date('5-Jun-2019','dd-Mon-yyyy'),to_date('15-Jun-2019','dd-Mon-yyyy'),1);
insert into AW_DISPLAY values(2,1,1,to_date('21-Jun-2019','dd-Mon-yyyy'),to_date('29-Jun-2019','dd-Mon-yyyy'),2);
insert into AW_DISPLAY values(3,2,1,to_date('6-Sep-2019','dd-Mon-yyyy'),to_date('15-Sep-2019','dd-Mon-yyyy'),2);
insert into AW_DISPLAY values(4,2,1,to_date('21-Sep-2019','dd-Mon-yyyy'),to_date('29-Sep-2019','dd-Mon-yyyy'),3);
insert into AW_DISPLAY values(5,3,2,to_date('8-Aug-2019','dd-Mon-yyyy'),to_date('17-Aug-2019','dd-Mon-yyyy'),1);
insert into AW_DISPLAY values(6,3,2,to_date('20-Aug-2019','dd-Mon-yyyy'),to_date('29-Aug-2019','dd-Mon-yyyy'),3);
insert into AW_DISPLAY values(7,4,1,to_date('9-Jun-2019','dd-Mon-yyyy'),to_date('18-Jun-2019','dd-Mon-yyyy'),2);
insert into AW_DISPLAY values(8,4,1,to_date('23-Jun-2019','dd-Mon-yyyy'),to_date('29-Jun-2019','dd-Mon-yyyy'),3);
insert into AW_DISPLAY values(9,5,1,to_date('14-Oct-2019','dd-Mon-yyyy'),to_date('20-Oct-2019','dd-Mon-yyyy'),2);
insert into AW_DISPLAY values(10,5,1,to_date('22-Oct-2019','dd-Mon-yyyy'),to_date('28-Oct-2019','dd-Mon-yyyy'),3);
insert into AW_DISPLAY values(11,6,3,to_date('24-Oct-2019','dd-Mon-yyyy'),to_date('5-Nov-2019','dd-Mon-yyyy'),2);
insert into AW_DISPLAY values(12,6,3,to_date('8-Nov-2019','dd-Mon-yyyy'),to_date('25-Nov-2019','dd-Mon-yyyy'),3);
insert into AW_DISPLAY values(13,7,1,to_date('15-Jun-2019','dd-Mon-yyyy'),to_date('25-Jun-2019','dd-Mon-yyyy'),3);
insert into AW_DISPLAY values(14,7,1,to_date('27-Jun-2019','dd-Mon-yyyy'),to_date('5-July-2019','dd-Mon-yyyy'),4);
insert into AW_DISPLAY values(15,8,1,to_date('15-Jun-2019','dd-Mon-yyyy'),to_date('25-Jun-2019','dd-Mon-yyyy'),4);
insert into AW_DISPLAY values(16,8,1,to_date('27-Jun-2019','dd-Mon-yyyy'),to_date('5-July-2019','dd-Mon-yyyy'),5);
insert into AW_DISPLAY values(17,9,1,to_date('18-Oct-2019','dd-Mon-yyyy'),to_date('28-Oct-2019','dd-Mon-yyyy'),5);
insert into AW_DISPLAY values(18,10,1,to_date('6-Jun-2020','dd-Mon-yyyy'),to_date('14-Jun-2020','dd-Mon-yyyy'),2);

commit;

insert into SALE values(1,to_date('29-Jun-2019','dd-Mon-yyyy'),1000,1,2);
insert into SALE values(2,to_date('29-Sep-2019','dd-Mon-yyyy'),2000,4,4);
insert into SALE values(3,to_date('29-Aug-2019','dd-Mon-yyyy'),3000,3,6);
insert into SALE values(4,to_date('25-Oct-2019','dd-Mon-yyyy'),24000,2,8);


commit;

insert into AW_STATUS values(1,1,1,to_date('2-Jun-2019 11:11','dd-Mon-yyyy HH24:Mi'),'W',null);
insert into AW_STATUS values(2,1,1,to_date('3-Jun-2019 12:11','dd-Mon-yyyy HH24:Mi'),'T',1);
insert into AW_STATUS values(3,1,1,to_date('4-Jun-2019 11:11','dd-Mon-yyyy HH24:Mi'),'G',1);
insert into AW_STATUS values(4,1,1,to_date('16-Jun-2019 12:11','dd-Mon-yyyy HH24:Mi'),'T',1);
insert into AW_STATUS values(5,1,1,to_date('17-Jun-2019 11:11','dd-Mon-yyyy HH24:Mi'),'W',null);
insert into AW_STATUS values(6,1,1,to_date('18-Jun-2019 12:11','dd-Mon-yyyy HH24:Mi'),'T',2);
insert into AW_STATUS values(7,1,1,to_date('20-Jun-2019 11:11','dd-Mon-yyyy HH24:Mi'),'G',2);
insert into AW_STATUS values(8,1,1,to_date('29-Jun-2019 10:11','dd-Mon-yyyy HH24:Mi'),'S',2);

insert into AW_STATUS values(9,2,1,to_date('4-Sep-2019 11:11','dd-Mon-yyyy HH24:Mi'),'W',null);
insert into AW_STATUS values(10,2,1,to_date('5-Sep-2019 12:11','dd-Mon-yyyy HH24:Mi'),'T',2);
insert into AW_STATUS values(11,2,1,to_date('6-Sep-2019 11:11','dd-Mon-yyyy HH24:Mi'),'G',2);
insert into AW_STATUS values(12,2,1,to_date('16-Sep-2019 12:11','dd-Mon-yyyy HH24:Mi'),'T',2);
insert into AW_STATUS values(13,2,1,to_date('17-Sep-2019 11:11','dd-Mon-yyyy HH24:Mi'),'W',null);
insert into AW_STATUS values(14,2,1,to_date('18-Sep-2019 12:11','dd-Mon-yyyy HH24:Mi'),'T',3);
insert into AW_STATUS values(15,2,1,to_date('20-Sep-2019 11:11','dd-Mon-yyyy HH24:Mi'),'G',3);
insert into AW_STATUS values(16,2,1,to_date('29-Sep-2019 10:11','dd-Mon-yyyy HH24:Mi'),'S',3);

insert into AW_STATUS values(17,3,2,to_date('5-Aug-2019 11:11','dd-Mon-yyyy HH24:Mi'),'W',null);
insert into AW_STATUS values(18,3,2,to_date('6-Aug-2019 12:11','dd-Mon-yyyy HH24:Mi'),'T',1);
insert into AW_STATUS values(19,3,2,to_date('7-Aug-2019 11:11','dd-Mon-yyyy HH24:Mi'),'G',1);
insert into AW_STATUS values(20,3,2,to_date('18-Aug-2019 12:11','dd-Mon-yyyy HH24:Mi'),'T',1);
insert into AW_STATUS values(21,3,2,to_date('19-Aug-2019 11:11','dd-Mon-yyyy HH24:Mi'),'W',null);
insert into AW_STATUS values(22,3,2,to_date('19-Aug-2019 16:11','dd-Mon-yyyy HH24:Mi'),'T',3);
insert into AW_STATUS values(23,3,2,to_date('20-Aug-2019 11:11','dd-Mon-yyyy HH24:Mi'),'G',3);
insert into AW_STATUS values(24,3,2,to_date('29-Aug-2019 10:11','dd-Mon-yyyy HH24:Mi'),'S',3);

insert into AW_STATUS values(25,4,1,to_date('6-Jun-2019 11:11','dd-Mon-yyyy HH24:Mi'),'W',null);
insert into AW_STATUS values(26,4,1,to_date('7-Jun-2019 12:11','dd-Mon-yyyy HH24:Mi'),'T',2);
insert into AW_STATUS values(27,4,1,to_date('8-Jun-2019 11:11','dd-Mon-yyyy HH24:Mi'),'G',2);
insert into AW_STATUS values(28,4,1,to_date('19-Jun-2019 12:11','dd-Mon-yyyy HH24:Mi'),'T',2);
insert into AW_STATUS values(29,4,1,to_date('20-Jun-2019 11:11','dd-Mon-yyyy HH24:Mi'),'W',null);
insert into AW_STATUS values(30,4,1,to_date('21-Jun-2019 12:11','dd-Mon-yyyy HH24:Mi'),'T',3);
insert into AW_STATUS values(31,4,1,to_date('22-Jun-2019 11:11','dd-Mon-yyyy HH24:Mi'),'G',3);
insert into AW_STATUS values(32,4,1,to_date('29-Jun-2019 10:11','dd-Mon-yyyy HH24:Mi'),'S',3);

insert into AW_STATUS values(33,5,1,to_date('12-Oct-2019 11:11','dd-Mon-yyyy HH24:Mi'),'W',null);
insert into AW_STATUS values(34,5,1,to_date('13-Oct-2019 12:11','dd-Mon-yyyy HH24:Mi'),'T',2);
insert into AW_STATUS values(35,5,1,to_date('14-Oct-2019 11:11','dd-Mon-yyyy HH24:Mi'),'G',2);
insert into AW_STATUS values(36,5,1,to_date('21-Oct-2019 12:11','dd-Mon-yyyy HH24:Mi'),'T',2);
insert into AW_STATUS values(37,5,1,to_date('21-Oct-2019 14:11','dd-Mon-yyyy HH24:Mi'),'W',null);
insert into AW_STATUS values(38,5,1,to_date('21-Oct-2019 16:11','dd-Mon-yyyy HH24:Mi'),'T',3);
insert into AW_STATUS values(39,5,1,to_date('22-Oct-2019 11:11','dd-Mon-yyyy HH24:Mi'),'G',3);
insert into AW_STATUS values(40,5,1,to_date('29-Oct-2019 10:11','dd-Mon-yyyy HH24:Mi'),'R',null);

insert into AW_STATUS values(41,6,3,to_date('22-Oct-2019 11:11','dd-Mon-yyyy HH24:Mi'),'W',null);
insert into AW_STATUS values(42,6,3,to_date('23-Oct-2019 12:11','dd-Mon-yyyy HH24:Mi'),'T',2);
insert into AW_STATUS values(43,6,3,to_date('24-Oct-2019 11:11','dd-Mon-yyyy HH24:Mi'),'G',2);
insert into AW_STATUS values(44,6,3,to_date('6-Nov-2019 12:11','dd-Mon-yyyy HH24:Mi'),'T',2);
insert into AW_STATUS values(45,6,3,to_date('6-Nov-2019 15:11','dd-Mon-yyyy HH24:Mi'),'W',null);
insert into AW_STATUS values(46,6,3,to_date('6-Nov-2019 18:11','dd-Mon-yyyy HH24:Mi'),'T',3);
insert into AW_STATUS values(47,6,3,to_date('7-Nov-2019 11:11','dd-Mon-yyyy HH24:Mi'),'G',3);
insert into AW_STATUS values(48,6,3,to_date('26-Nov-2019 10:11','dd-Mon-yyyy HH24:Mi'),'R',null);

insert into AW_STATUS values(49,7,1,to_date('12-Jun-2019 11:11','dd-Mon-yyyy HH24:Mi'),'W',null);
insert into AW_STATUS values(50,7,1,to_date('13-Jun-2019 12:11','dd-Mon-yyyy HH24:Mi'),'T',3);
insert into AW_STATUS values(51,7,1,to_date('14-Jun-2019 11:11','dd-Mon-yyyy HH24:Mi'),'G',3);
insert into AW_STATUS values(52,7,1,to_date('26-Jun-2019 12:11','dd-Mon-yyyy HH24:Mi'),'T',3);
insert into AW_STATUS values(53,7,1,to_date('26-Jun-2019 15:11','dd-Mon-yyyy HH24:Mi'),'W',null);
insert into AW_STATUS values(54,7,1,to_date('26-Jun-2019 17:11','dd-Mon-yyyy HH24:Mi'),'T',4);
insert into AW_STATUS values(55,7,1,to_date('27-Jun-2019 11:11','dd-Mon-yyyy HH24:Mi'),'G',4);
insert into AW_STATUS values(56,7,1,to_date('6-July-2019 10:11','dd-Mon-yyyy HH24:Mi'),'R',null);

insert into AW_STATUS values(57,8,1,to_date('12-Jun-2019 11:11','dd-Mon-yyyy HH24:Mi'),'W',null);
insert into AW_STATUS values(58,8,1,to_date('13-Jun-2019 12:11','dd-Mon-yyyy HH24:Mi'),'T',4);
insert into AW_STATUS values(59,8,1,to_date('14-Jun-2019 10:11','dd-Mon-yyyy HH24:Mi'),'G',4);
insert into AW_STATUS values(60,8,1,to_date('26-Jun-2019 12:11','dd-Mon-yyyy HH24:Mi'),'T',4);
insert into AW_STATUS values(61,8,1,to_date('26-Jun-2019 14:11','dd-Mon-yyyy HH24:Mi'),'W',null);
insert into AW_STATUS values(62,8,1,to_date('26-Jun-2019 16:11','dd-Mon-yyyy HH24:Mi'),'T',5);
insert into AW_STATUS values(63,8,1,to_date('27-Jun-2019 11:11','dd-Mon-yyyy HH24:Mi'),'G',5);
insert into AW_STATUS values(64,8,1,to_date('6-July-2019 10:11','dd-Mon-yyyy HH24:Mi'),'R',null);

insert into AW_STATUS values(65,9,1,to_date('15-Oct-2019 11:11','dd-Mon-yyyy HH24:Mi'),'W',null);
insert into AW_STATUS values(66,9,1,to_date('16-Oct-2019 12:11','dd-Mon-yyyy HH24:Mi'),'T',5);
insert into AW_STATUS values(67,9,1,to_date('17-Oct-2019 11:11','dd-Mon-yyyy HH24:Mi'),'G',5);
insert into AW_STATUS values(68,9,1,to_date('29-Oct-2019 10:11','dd-Mon-yyyy HH24:Mi'),'R',null);

insert into AW_STATUS values(69,10,1,to_date('3-Jun-2020 11:11','dd-Mon-yyyy HH24:Mi'),'W',null);
insert into AW_STATUS values(70,10,1,to_date('4-Jun-2020 12:11','dd-Mon-yyyy HH24:Mi'),'T',2);
insert into AW_STATUS values(71,10,1,to_date('5-Jun-2020 11:11','dd-Mon-yyyy HH24:Mi'),'G',2);
insert into AW_STATUS values(72,10,1,to_date('15-Jun-2020 10:11','dd-Mon-yyyy HH24:Mi'),'R',null);

insert into AW_STATUS values(73,10,2,to_date('3-Jun-2020 11:11','dd-Mon-yyyy HH24:Mi'),'W',null);


commit;


-- explain analyze
# Create a CTE to calculate the average grade per course 
# Only include courses where the lowest grade is at least 45
with AvgGradeByCourses as(
	SELECT
	    AVG(e.grade) AS average_grade,
	    e.course_id,
	    c.course_name
	FROM enrollments as e
	join courses c on e.course_id = c.course_id 
	GROUP BY c.course_id    
 	having min(e.grade) >= 45
)
#
# Select detailed student enrollment info for courses with high average grades
#
select 
    s.student_id,
    concat(s.first_name, ' ', s.last_name) as student_name,
    s.major,
    c.course_name,
    c.department,
    e.grade,
    e.semester,
    concat(p.first_name, ' ', p.last_name) as professor_name,
    p.department AS professor_department
from enrollments e
#
# Join all tables we needed
#
join students s on e.student_id = s.student_id #note join is inner join
join courses c on e.course_id = c.course_id
join course_assignments ca on c.course_id = ca.course_id
join professors p on ca.professor_id = p.professor_id
#
# Join with the CTE to filter for courses with average grade > 70
#
join AvgGradeByCourses agc
where agc.average_grade > 70 
	and agc.course_name = c.course_name 
	# Filter for specific semester (Fall 2023)
	and e.semester in (
		select semester
		from enrollments
		where semester = 'Fall 2023'
	)
#
# Sort results: highest grades first, then by student ID and semester
# Limit output to top 10 rows
order by e.grade desc, s.student_id, e.semester
limit 10
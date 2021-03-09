-- flair,
explain select 
    author,
    subreddit,
    flair,
    count(*) as posts,
    sum(score)    
from comment 
where author in (
    select distinct(author)
    from comment
    where subreddit = 'DotA2'
)
group by author,subreddit,flair ;

CREATE INDEX idx_subreddit_comment
on comment(subreddit);

select count(subreddit) from comment;


CREATE INDEX idx_author_subreddit_comment_score
on comment(author,subreddit,flair,score);

VACUUM Analyze comment;

------------------------------ COMMANDS -------------
1. create index 
CREATE INDEX idx_author_subreddit_flair_prediction
on comment(author,subreddit,flair,score);

2. create table form query
create table pcm_flair_prediction_1 as (select 
    author,
    subreddit,
    flair,
    count(*) as posts,
    sum(score)    
from comment 
where author in (
    select distinct(author)
    from comment
    where subreddit = 'PoliticalCompassMemes'
    and created_utc > 1561153400
)
group by author,subreddit,flair) ;

CREATE INDEX idx_ppm_flair_prediction_author
on ppm_flair_prediction(author);

CREATE INDEX idx_ppm_flair_prediction_subreddit
on ppm_flair_prediction(subreddit);

3. Dont list people that have changed flair
-> dipersion is 50 users
select * 
from ppm_flair_prediction
where author in (
    select author
    from 
    (select author,flair, count(*)
        from pcm_flair_prediction_1
        where subreddit = 'PoliticalCompassMemes'
        and flair is not null
        and flair != 0
        group by author, flair) as t1
    group by author
    having count(*) = 1
) and subreddit in (
    select subreddit
    from ppm_flair_prediction
    group by subreddit
    having count(*) >= 50
);


-------------- test

create table askreddit_flair_prediction as (select 
        author,
        subreddit,
        flair,
        count(*) as posts,
        sum(score)    
    from comment 
    where author in (
        select distinct(author)
        from comment
        where subreddit = 'AskReddit'
    )
    group by author,subreddit,flair
);


select * 
from askreddit_flair_prediction  
where author in (
    select author
    from 
    (select author,flair, count(*)
        from askreddit_flair_prediction
        group by author, flair) as t1
    group by author
    having count(*) = 1
) and subreddit in (
    select subreddit
    from askreddit_flair_prediction
    group by subreddit
    having count(*) > 50
);
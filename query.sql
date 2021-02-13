-- flair,
explain select 
    author,
    subreddit,
    count(*) as posts,
    sum(score)    
from comment 
where author in (
    select distinct(author)
    from comment
    where subreddit = 'DotA2'
)
group by author,subreddit ;

CREATE INDEX idx_subreddit_comment
on comment(subreddit);

CREATE INDEX idx_subreddit_comment
on comment(subreddit);

select count(subreddit) from comment;


CREATE INDEX idx_author_subreddit_comment_score
on comment(author,subreddit,score);

VACUUM Analyze comment;

# Quickwit Coding Style

This document resumes a couple of points we try to embrace in our coding style. Some of these points take an opinionated side on a trade-off story.
The description will try to make that clear.

The driving motivation of this code style is to make your code more readable.

Readable is one word that hides several dimensions:
- the reader understands the intent very rapidly
- the reader can proofread. It can become confident that the code is correct very easily.

Noticing how the two are different should not require too much squinting.
Shoot for *proofreadability*.

## Code reviews

Do a pass on your own code before sending it for review to avoid wasting the review time.
Also, a trivial code style issues can come in the way and avoid spotting
deeper issues with the code.

As a reviewer, your first mission is proofreading. If you find a logical bug, feel good. You did an awesome job today.

Your second goal is to make sure the code quality stays high.

You can express "nitpicks": suggestions about some local aspect of the code that do not matter too much. Just prepend "nitpick:" to your comment.
You can also express an opinion/advice that you know is not universal.
Make sure you make it clear to the reviewee that it is fine to ignore the comment.

Do not use rhetorical questions... If you are 95% sure of something, there is no need to express it as a question.
Prefer `I believe this should be n+1` to `Shouldn't this be n+1?`.

The issue with rhetorical questions is that when you will have a genuine
question, reviewees may over interpret it as an affirmation.

As a reviewee, if you are not used to CRs, it can feel like an adversarial process. Relax. This is normal to end up with a lot of comments on your first few CRs.

You might feel like the comments are unjustified, try as much as possible to not feel frustrated.
If you want to discuss it, the best place is the chat, or maybe send a PR to modify this document.

But remember to pick your battles... If you think it does not matter much but it takes 2 secs to fix, just consider doing what is suggested by the reviewer or this style guide.

## Rust gives us a lot of tools... this does not mean we need to abuse them.

Rust is an amazing language. It offers all kinds of tools to allow for zero-cost code reuse. Within these tools, however, generics and macros tend to hurt readability (and compile-time). Let's ONLY use them where necessary.

The same goes with the chaining iterator style.
When coupled with error handling, rust's chaining iterator style can
hurt readability.
Using a good old procedural for-loop is fine and recommended in that case.

**example needed**


## Naming

Function and variable names are key for readability.

A good function name is often sufficient for the reader to build reasonable expectations of what it does.

If this implies long names, let's have very long names.

Trying to fit this rule has an interesting side effect.
Nobody likes to type long function names. It just feels ugly.
But these are frequently symptoms of a badly organized code, and it can
help spot refactoring opportunities.

**example needed**

## Explanatory variables

One incredibly powerful tool and simple tool to help make your code
more readable is to introduce explanatory variables.

Explanatory variables are intermediary variables that were not really
necessary, but make it possible -through their names- to convey their
semantics to the reader.

**example needed**

## Shadowing

As much as possible, do not use reuse the same variable name in a function.
It is never necessary, very rarely helpful and can hurt.

## Types

Rust handles type elision. That's great.
Chances are, your editor even automatically hints the type of
your variables.

Sometimes, however, it can be helpful for the reviewer to have the type of some very strategic variables.

**example needed**

## Early returns

We prefer early return.
Rather than chaining `else` statement, we prefer to isolate
corner case in short `if` statement to prevent nesting

**example needed**

## Invariants

A good idea to help reviewers proofread your code is to
identify invariants and express them as `debug_assert`.

These assert will not be part of the release binary and won't hurt the execution time.

**example needed**

## Comments

We use on the same code style, [rustc's doc comments](https://doc.rust-lang.org/1.0.0/style/style/comments.html).
In particular, the summary line should be written in third-person singular present indicative form.

No rustdoc in Quickwit or in private API is ok.
No rustdoc on Tantivy public API is not ok.

We usually do not expect comments to contain any implementation details.
To some extent, it is normal for the user to have to look at the code.

When it is not clear, comments should convey:
- intent
- context (links to a Wikipedia page or a paper, link to the original issue can be helpful too)
- hidden contracts... but really you should avoid those.

Inline comments in the code can be very useful to help the reader understand
the justification of a thorny piece of code.

**example needed**

## Hidden contracts

We call hidden contract, a pre-condition on the arguments that is not enforced by their types.

Sometimes, hidden contracts are unavoidable.

For instance, a binary search requires the array to be sorted.

Whenever possible, you should avoid having hidden contracts.

To avoid hidden contracts, you should consider:
- changing your argument types to have the type system enforce the contract
- internalize the contract enforcement.

For instance, the following function is not good because it hides a contract on values not being empty:

```
fn min(&self, values: &[usize]) -> usize {
	let mut min_val = usize::MAX;
	for val in values {
		min_val = min_val.min(val)
	}
	min_val
}
```
It can be done by changing the prototype to a `Result` or an `Option`.

In addition, while the author might have thought that the `usize::MAX` trick was a nice touch, it can easily backfire. Panicking is often better than returning a wrong result.

The better approach here is of course an `Option<usize>` like `Iterator::min` does.

Another way to internalize the contract enforcement is to move some logic from the caller to within the function.

For instance:
```
// The algorithms requires splits to be sorted by `end_time`
fn merge_candidates(splits: &mut Vec<SplitMetadata>) -> Vec<SplitMetadata>
```

It is tempting to rely on the fact that splits `Vec` is always sorted on the caller side and put this as a hidden contract.
If it is not too much work, just redoing the sorting within merge candidates
is a good idea. For the above function, that extra work is tiny.

By the way, did you know Rust's std sort is inspired by timsort?
It will perform in linear time if the array is already sorted...

When implementing a function with a hidden contract, as long as it does not hurt the overall performance, add an assert statement to your code to check the contract. (For instance, check that the array is sorted).

**example needed*

## Tests

Test do not need to match the same quality as the original code.

When a bug is encountered, it is ok to introduce a test that seems weirdly
overfitted to the specific issue. A comment should then add a link to the issue.

Unit test should run fast, and if possible they should not do any IO.
Code should be structured to make unit testing possible.

Some of our unit tests would not be considered good unit tests in some companies, and that's ok.

Here are the controversial bits:

### Not just for spotting regression

Our unit tests are not here just to spot regression.
They are also here to check the correctness of our code.

### Not just testing public API

Unit test do not only test public API.
Complex code often calls half a dozen smaller functions.

The cardinality of the corner case of the complex code
can make it difficult to test all corner case.

On the other hand, the smaller functions could be tested
exhaustively.

For this reason, testing internal private functions is actually encouraged.

### Not always "unit" tests

Ideally, unit tests should be testing one thing and one thing only, but if they don't and it helps cover more ground, this is ok.

### Not necessarily deterministic.

Finally, unit tests are not necessarily deterministic. We really like proptests.
When proptesting, make sure to reduce as much as possible the space of exploration to get the most out of it.

## async vs sync

Your async code should block for at most 500 microseconds.
If you are unsure whether your code blocks for 500 microseconds, or if it is a non-trivial question, it should run via `tokio::spawn_block`.

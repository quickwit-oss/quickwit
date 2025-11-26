// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::time::Duration;

pub struct PrettySample<I>(I, usize);

impl<I> PrettySample<I> {
    pub fn new(slice: I, sample_size: usize) -> Self {
        Self(slice, sample_size)
    }
}

impl<I> fmt::Debug for PrettySample<I>
where
    I: IntoIterator + Clone,
    I::Item: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "[")?;

        // In general, we will receive a reference (&[...], &HashMap...) or a Map<_> of them.
        // So we either perform a Copy, or a cheap Clone of a simple struct
        let mut iter = self.0.clone().into_iter().enumerate();
        for (i, item) in &mut iter {
            if i > 0 {
                write!(formatter, ", ")?;
            }
            write!(formatter, "{item:?}")?;
            if i == self.1 - 1 {
                break;
            }
        }
        let left = iter.count();
        if left > 0 {
            write!(formatter, ", and {left} more")?;
        }
        write!(formatter, "]")?;
        Ok(())
    }
}

pub trait PrettyDisplay {
    fn pretty_display(&self) -> impl fmt::Display;
}

struct DurationPrettyDisplay<'a>(&'a Duration);

impl fmt::Display for DurationPrettyDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        // This is enough for my current use cases. To be extended as you see fit.
        let duration_millis = self.0.as_millis();

        if duration_millis < 1_000 {
            return write!(formatter, "{duration_millis}ms");
        }
        write!(
            formatter,
            "{}.{}s",
            duration_millis / 1_000,
            duration_millis % 1_000 / 10
        )
    }
}

impl PrettyDisplay for Duration {
    fn pretty_display(&self) -> impl fmt::Display {
        DurationPrettyDisplay(self)
    }
}

struct SequencePrettyDisplay<I>(I);

impl<I> fmt::Display for SequencePrettyDisplay<I>
where
    I: IntoIterator + Clone,
    I::Item: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;

        // In general, we will receive a reference (&[...], &HashMap...) or a Map<_> of them.
        // So we either perform a Copy, or a cheap Clone of a simple struct
        let mut iter = self.0.clone().into_iter().peekable();

        while let Some(item) = iter.next() {
            write!(f, "{item}")?;
            if iter.peek().is_some() {
                write!(f, ", ")?;
            }
        }
        write!(f, "]")
    }
}

impl<T: fmt::Display> PrettyDisplay for &[T] {
    fn pretty_display(&self) -> impl fmt::Display {
        SequencePrettyDisplay(*self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pretty_sample() {
        let pretty_sample = PrettySample::<&[usize]>::new(&[], 2);
        assert_eq!(format!("{pretty_sample:?}"), "[]");

        let pretty_sample = PrettySample::new(&[1], 2);
        assert_eq!(format!("{pretty_sample:?}"), "[1]");

        let pretty_sample = PrettySample::new(&[1, 2], 2);
        assert_eq!(format!("{pretty_sample:?}"), "[1, 2]");

        let pretty_sample = PrettySample::new(&[1, 2, 3], 2);
        assert_eq!(format!("{pretty_sample:?}"), "[1, 2, and 1 more]");

        let pretty_sample = PrettySample::new(&[1, 2, 3, 4], 2);
        assert_eq!(format!("{pretty_sample:?}"), "[1, 2, and 2 more]");
    }

    #[test]
    fn test_duration_pretty_display() {
        let duration = Duration::from_millis(0);
        assert_eq!(format!("{}", duration.pretty_display()), "0ms");

        let duration = Duration::from_millis(125);
        assert_eq!(format!("{}", duration.pretty_display()), "125ms");

        let duration = Duration::from_millis(1_000);
        assert_eq!(format!("{}", duration.pretty_display()), "1.0s");

        let duration = Duration::from_millis(1_125);
        assert_eq!(format!("{}", duration.pretty_display()), "1.12s");
    }

    #[test]
    fn test_sequence_pretty_display() {
        let empty_slice: &[i32] = &[];
        assert_eq!(format!("{}", empty_slice.pretty_display()), "[]");

        let slice_one: &[i32] = &[1];
        assert_eq!(format!("{}", slice_one.pretty_display()), "[1]");

        let slice_two: &[i32] = &[1, 2];
        assert_eq!(format!("{}", slice_two.pretty_display()), "[1, 2]");
    }
}

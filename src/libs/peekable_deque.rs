use std::collections::VecDeque;

#[derive(Debug)]
pub struct PeekableDeque<T> {
    deque: Vec<T>,
    index: usize,
}

impl<T> PeekableDeque<T> {
    // Constructor to create a new PeekableDeque from an iterator
    pub fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        PeekableDeque {
            deque: iter.into_iter().collect(),
            index: 0,
        }
    }

    // Method to get the next item and remove it from the deque
    pub fn next(&mut self) -> Option<&T> {
        self.index += 1;
        self.deque.get(self.index)
    }

    // Method to peek at the next item without removing it
    pub fn peek(&self) -> Option<&T> {
        self.deque.get(self.index)
    }

    pub fn back(&mut self, n: usize) {
        self.index = self.index.saturating_sub(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_back_negative() {
        let query = "test".to_string();
        let mut peekable_query = PeekableDeque::from_iter(query.chars());

        peekable_query.next();
        peekable_query.next();
        peekable_query.back(5);

        assert_eq!('t', *peekable_query.peek().unwrap());
    }

    #[test]
    fn test_back_positive() {
        let query = "test".to_string();
        let mut peekable_query = PeekableDeque::from_iter(query.chars());

        peekable_query.next();
        peekable_query.next();
        peekable_query.back(1);

        assert_eq!('e', *peekable_query.peek().unwrap());
    }

    #[test]
    fn test_peek() {
        let query = "test".to_string();
        let peekable_query = PeekableDeque::from_iter(query.chars());

        if let Some(peeked_char) = peekable_query.peek() {
            if *peeked_char != 't' {
                panic!("Expected 't' char, but got {}", peeked_char);
            }
        } else {
            panic!("Expected 't' char, but got nothing");
        }
    }

    #[test]
    fn test_peek_when_empty() {
        let query = "".to_string();
        let peekable_query = PeekableDeque::from_iter(query.chars());

        if let Some(peeked_char) = peekable_query.peek() {
            panic!("Expected nothing, but got {}", peeked_char);
        }
    }

    #[test]
    fn test_next() {
        let query = "test".to_string();
        let mut peekable_query = PeekableDeque::from_iter(query.chars());

        assert_eq!('e', *peekable_query.next().unwrap());
        assert_eq!('e', *peekable_query.peek().unwrap());
    }

    #[test]
    fn test_next_when_empty() {
        let query = "".to_string();
        let mut peekable_query = PeekableDeque::from_iter(query.chars());

        peekable_query.next();

        if let Some(peeked_char) = peekable_query.peek() {
            panic!("Expected nothing, but got {}", peeked_char);
        }
    }
}

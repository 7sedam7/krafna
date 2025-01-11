use std::collections::VecDeque;

#[derive(Debug)]
pub struct PeekableDeque<T> {
    deque: VecDeque<T>,
}

impl<T> PeekableDeque<T> {
    // Constructor to create a new PeekableDeque from an iterator
    pub fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        PeekableDeque {
            deque: iter.into_iter().collect(),
        }
    }

    // Method to get the next item and remove it from the deque
    pub fn next(&mut self) -> Option<T> {
        self.deque.pop_front()
    }

    // Method to peek at the next item without removing it
    pub fn peek(&self) -> Option<&T> {
        self.deque.front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        peekable_query.next();

        if let Some(peeked_char) = peekable_query.peek() {
            if *peeked_char != 'e' {
                panic!("Expected 'e' char, but got {}", peeked_char);
            }
        } else {
            panic!("Expected 'e' char, but got nothing");
        }
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

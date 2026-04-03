import { render, screen } from '@testing-library/react';
import App from './App';

test('renders distributed transaction monitor heading', () => {
  render(<App />);
  const headingElement = screen.getByText(/distributed transaction monitor/i);
  expect(headingElement).toBeInTheDocument();
});

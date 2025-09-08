import React, { useState } from 'react';
import './QuestionBox.css';

interface QuestionBoxProps {
  onSubmit?: (question: string) => void;
}

const QuestionBox: React.FC<QuestionBoxProps> = ({ onSubmit }) => {
  const [question, setQuestion] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [result, setResult] = useState<string>('');
  const [answers, setAnswers] = useState<Array<{ question: string; valid: boolean; descriptor: string }>>([]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    const trimmed = question.trim();
    if (!trimmed) return;
    try {
      setIsSubmitting(true);
      if (onSubmit) onSubmit(trimmed);
      // Call backend iterative tool-calling endpoint
      try {
        // Multi-question endpoint to split and answer per sentence
        const resp = await fetch('http://localhost:8000/api/query/ask_multi', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ text: trimmed, max_loops: 3 })
        });
        const data = await resp.json();
        if (data && data.status === 'success' && Array.isArray(data.results)) {
          setAnswers(data.results.map((r: any) => ({ question: r.question, valid: !!r.valid, descriptor: r.descriptor })));
          setResult('');
        } else {
          setResult(`Error | ${data?.message || 'Unknown error'}`);
          setAnswers([]);
        }
      } catch (e: any) {
        setResult(`Error | ${e?.message || 'Request failed'}`);
        setAnswers([]);
      }
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="question-box">
      <h3 className="question-title">Ask a Question</h3>
      <p className="question-subtitle">Describe what you want to know about the graph.</p>
      <form onSubmit={handleSubmit} className="question-form">
        <textarea
          className="question-textarea"
          placeholder="e.g., Show relationships between Company X and Person Y in 2019"
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          rows={8}
        />
        <div className="question-actions">
          <button type="submit" disabled={isSubmitting || question.trim().length === 0}>
            {isSubmitting ? 'Submitting…' : 'Submit'}
          </button>
        </div>
      </form>
      {result && (
        <div style={{ textAlign: 'left', fontSize: '0.9rem', color: '#333' }}>
          <strong>Result:</strong> {result}
        </div>
      )}
      {answers.length > 0 && (
        <div className="answers-box">
          <div style={{ fontWeight: 600, marginBottom: 6 }}>Answers</div>
          {answers.map((a, i) => (
            <div key={i} className="answer-item">
              <div style={{ opacity: 0.7 }}>Q: {a.question}</div>
              <div>A: {(a.valid ? 'True' : 'False') + ' | ' + (a.descriptor || '')}</div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

export default QuestionBox;



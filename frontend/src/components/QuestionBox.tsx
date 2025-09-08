import React, { useState } from 'react';
import './QuestionBox.css';
const API_BASE_URL = (process.env.REACT_APP_API_BASE_URL || 'http://localhost:8000').replace(/\/+$/, '');

interface QuestionBoxProps {
  onSubmit?: (question: string) => void;
}

const QuestionBox: React.FC<QuestionBoxProps> = ({ onSubmit }) => {
  const [question, setQuestion] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [result, setResult] = useState<string>('');
  const [answers, setAnswers] = useState<Array<{ question: string; valid: boolean; descriptor: string }>>([]);

  const isYesNoQuestion = (q: string): boolean => {
    const trimmed = (q || '').trim();
    if (!trimmed) return false;
    const firstWord = trimmed.split(/\s+/)[0].toLowerCase();
    const auxiliaries = [
      'is','are','am','was','were','do','does','did','can','could','should','would','will','has','have','had','may','might','shall','must'
    ];
    return auxiliaries.includes(firstWord);
  };

  const stripLeadingYesNo = (text: string): string => {
    if (!text) return '';
    // Remove a leading Yes/No/True/False with common punctuation
    return text.replace(/^(\s*)(yes|no|true|false)[\s.:,\-\u2013\u2014]+/i, '$1').trimStart();
  };

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
        const resp = await fetch(`${API_BASE_URL}/api/query/ask_multi`, {
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
      <h3 className="question-title subheader">Ask a Question</h3>
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
          <div style={{ fontWeight: 700, marginBottom: 8, fontSize: '1.5rem' }}>Answers</div>
          {answers.map((a, i) => {
            const showYesNo = isYesNoQuestion(a.question);
            const text = showYesNo ? (a.descriptor || '') : stripLeadingYesNo(a.descriptor || '');
            return (
              <div key={i} className="answer-item">
                <div style={{ opacity: 0.7 }}>Q: {a.question}</div>
                <div>A: {text}</div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
};

export default QuestionBox;



import { NextResponse } from 'next/server';
import OpenAI from 'openai';

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

export async function POST(req: Request) {
  try {
    const { prompt } = await req.json();

    if (!prompt) {
      return NextResponse.json({ error: 'Prompt is required' }, { status: 400 });
    }

    const response = await openai.chat.completions.create({
      model: 'gpt-4o', // 또는 'gpt-3.5-turbo'
      messages: [
        { role: 'system', content: '당신은 자동차 부품 전문가입니다.' },
        { role: 'user', content: prompt },
      ],
    });

    return NextResponse.json({ 
      result: response.choices[0].message.content 
    });
  } catch (error: any) {
    console.error('OpenAI API Error:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

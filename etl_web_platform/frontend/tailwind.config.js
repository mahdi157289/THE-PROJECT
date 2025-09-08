/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      fontFamily: {
        'sans': ['Inter', 'system-ui', 'sans-serif'],
        'display': ['Poppins', 'system-ui', 'sans-serif'],
        'mono': ['JetBrains Mono', 'monospace'],
        'roboto': ['Roboto', 'system-ui', 'sans-serif'],
        'montserrat': ['Montserrat', 'system-ui', 'sans-serif'],
        'serif': ['Source Serif 4', 'serif'],
      },
      colors: {
        // Enhanced color palette with better whites
        'light-green': '#90EE90', // Light green
        'light-silver': '#C0C0C0', // Light silver
        'pure-white': '#FFFFFF', // Pure white
        'crystal-white': '#F8FAFC', // Crystal white
        'pearl-white': '#F1F5F9', // Pearl white
        'dark-bg': '#000000', // Black background
        
        // Semantic color mapping
        primary: {
          50: '#90EE90', // Light green
          100: '#98FB98', // Pale green
          500: '#90EE90', // Light green
          600: '#7CFC00', // Lawn green
          700: '#32CD32', // Lime green
        },
        secondary: {
          50: '#C0C0C0', // Light silver
          100: '#D3D3D3', // Light gray
          500: '#C0C0C0', // Light silver
          600: '#A9A9A9', // Dark gray
          700: '#808080', // Gray
        },
        background: {
          primary: '#000000', // Black background
          secondary: '#1A1A1A', // Dark gray
          tertiary: '#2D2D2D', // Lighter dark gray
        },
        text: {
          primary: '#FFFFFF', // Pure white text
          secondary: '#F8FAFC', // Crystal white text
          accent: '#90EE90', // Light green accent
          muted: '#E2E8F0', // Muted white
        }
      },
      backgroundColor: {
        'main-bg': '#000000',
        'card-bg': '#1A1A1A',
        'hover-bg': '#2D2D2D',
      },
      textColor: {
        'main-text': '#FFFFFF',
        'secondary-text': '#F8FAFC',
        'accent-text': '#90EE90',
        'muted-text': '#E2E8F0',
      },
      borderColor: {
        'main-border': '#C0C0C0',
        'accent-border': '#90EE90',
      },
      animation: {
        'fade-in': 'fadeIn 0.5s ease-in-out',
        'slide-up': 'slideUp 0.3s ease-out',
        'slide-down': 'slideDown 0.3s ease-out',
        'scale-in': 'scaleIn 0.2s ease-out',
        'bounce-in': 'bounceIn 0.6s ease-out',
        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'glow': 'glow 2s ease-in-out infinite alternate',
      },
      keyframes: {
        fadeIn: {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' },
        },
        slideUp: {
          '0%': { transform: 'translateY(20px)', opacity: '0' },
          '100%': { transform: 'translateY(0)', opacity: '1' },
        },
        slideDown: {
          '0%': { transform: 'translateY(-20px)', opacity: '0' },
          '100%': { transform: 'translateY(0)', opacity: '1' },
        },
        scaleIn: {
          '0%': { transform: 'scale(0.95)', opacity: '0' },
          '100%': { transform: 'scale(1)', opacity: '1' },
        },
        bounceIn: {
          '0%': { transform: 'scale(0.3)', opacity: '0' },
          '50%': { transform: 'scale(1.05)' },
          '70%': { transform: 'scale(0.9)' },
          '100%': { transform: 'scale(1)', opacity: '1' },
        },
        glow: {
          '0%': { boxShadow: '0 0 5px #90EE90' },
          '100%': { boxShadow: '0 0 20px #90EE90, 0 0 30px #90EE90' },
        },
      },
      transitionProperty: {
        'all': 'all',
        'colors': 'color, background-color, border-color, text-decoration-color, fill, stroke',
        'opacity': 'opacity',
        'shadow': 'box-shadow',
        'transform': 'transform',
      },
    },
  },
  plugins: [],
}

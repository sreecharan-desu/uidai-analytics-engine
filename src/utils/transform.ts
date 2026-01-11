export const toTitleCase = (str: string): string => {
  // Handle common variations like '&' -> 'and' before title casing
  const normalized = str.toLowerCase().replace(/\s*&\s*/g, ' and ');
  return normalized
    .split(' ')
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
};
